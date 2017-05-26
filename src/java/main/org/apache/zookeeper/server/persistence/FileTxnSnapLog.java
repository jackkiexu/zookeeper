/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.persistence;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a helper class 
 * above the implementations 
 * of txnlog and snapshot 
 * classes
 *
 * 事务日志 快照日志的工具类
 *
 */
public class FileTxnSnapLog {
    //the direcotry containing the 
    //the transaction logs
    // 事务日志目录
    private final File dataDir;
    //the directory containing the
    //the snapshot directory
    // 快照日志目录
    private final File snapDir;
    // 事务日志
    private TxnLog txnLog;
    // 快照日志
    private SnapShot snapLog;
    // 版本号
    public final static int VERSION = 2;
    public final static String version = "version-";
    
    private static final Logger LOG = LoggerFactory.getLogger(FileTxnSnapLog.class);
    
    /**
     * This listener helps
     * the external apis calling
     * restore to gather information
     * while the data is being 
     * restored.
     */
    public interface PlayBackListener {
        void onTxnLoaded(TxnHeader hdr, Record rec);
    }
    
    /**
     * the constructor which takes the datadir and 
     * snapdir.
     * @param dataDir the trasaction directory
     * @param snapDir the snapshot directory
     */
    public FileTxnSnapLog(File dataDir, File snapDir) throws IOException {
        LOG.debug("Opening datadir:{} snapDir:{}", dataDir, snapDir);
        // 在对应目录下面生成 version-2 目录
        this.dataDir = new File(dataDir, version + VERSION);
        this.snapDir = new File(snapDir, version + VERSION);

        if (!this.dataDir.exists()) {
            if (!this.dataDir.mkdirs()) {
                throw new IOException("Unable to create data directory "
                        + this.dataDir);
            }
        }
        if (!this.snapDir.exists()) {
            if (!this.snapDir.mkdirs()) {
                throw new IOException("Unable to create snap directory "
                        + this.snapDir);
            }
        }

        // 初始化事务日志, snap日志
        txnLog = new FileTxnLog(this.dataDir);
        snapLog = new FileSnap(this.snapDir);
    }
    
    /**
     * get the datadir used by this filetxn
     * snap log
     * @return the data dir
     */
    public File getDataDir() {
        return this.dataDir;
    }
    
    /**
     * get the snap dir used by this 
     * filetxn snap log
     * @return the snap dir
     */
    public File getSnapDir() {
        return this.snapDir;
    }
    
    /**
     * this function restores the server 
     * database after reading from the 
     * snapshots and transaction logs
     * @param dt the datatree to be restored
     * @param sessions the sessions to be restored
     * @param listener the playback listener to run on the 
     * database restoration
     * @return the highest zxid restored
     * @throws IOException
     */
    // 将 Sessions 与 DataTree 从 数据流中恢复过来
    public long restore(DataTree dt, Map<Long, Integer> sessions,  PlayBackListener listener) throws IOException {

        snapLog.deserialize(dt, sessions);                              // 将对应的 sessions 与 DataTree 从 snapsot 中反序列化出来

        FileTxnLog txnLog = new FileTxnLog(dataDir);                    // 初始化事务日志
        TxnIterator itr = txnLog.read(dt.lastProcessedZxid+1);          // 将 cursor 定位到 DataTree 中处理的 lastProcessedZxid + 1 (为什么呢? 因为从事务日志里面还有些事务处理信息没有来得及存储到 DataTree, 接下来就是重新发到 DataTree 里面)
        long highestZxid = dt.lastProcessedZxid;
        TxnHeader hdr;
        try {
            while (true) {                                              // 迭代 TxnLog 里面的事务(这些事务信息还没在)
                // iterator points to 
                // the first valid txn when initialized
                hdr = itr.getHeader();
                if (hdr == null) {
                    //empty logs 
                    return dt.lastProcessedZxid;
                }
                if (hdr.getZxid() < highestZxid && highestZxid != 0) {
                    LOG.error("{}(higestZxid) > {}(next log) for type {}",
                            new Object[] { highestZxid, hdr.getZxid(),
                                    hdr.getType() });
                } else {
                    highestZxid = hdr.getZxid();                        // 记录处理过的最大 zxid
                }
                try {
                    processTransaction(hdr,dt,sessions, itr.getTxn());  // 将事务信息给 DataTree 处理
                } catch(KeeperException.NoNodeException e) {
                   throw new IOException("Failed to process transaction type: " +
                         hdr.getType() + " error: " + e.getMessage(), e);
                }
                listener.onTxnLoaded(hdr, itr.getTxn());                // 这里的 listener 会将集群节点最近处理掉 500 个事务信息放入其中, 在 Follower 与 Leader 同步事务时, 首先会从这里面去拿数据
                if (!itr.next()) 
                    break;
            }
        } finally {
            if (itr != null) {
                itr.close();
            }
        }
        return highestZxid;
    }
    
    /**
     * process the transaction on the datatree
     * @param hdr the hdr of the transaction
     * @param dt the datatree to apply transaction to
     * @param sessions the sessions to be restored
     * @param txn the transaction to be applied
     */
    public void processTransaction(TxnHeader hdr,DataTree dt,
            Map<Long, Integer> sessions, Record txn)
        throws KeeperException.NoNodeException {
        ProcessTxnResult rc;
        switch (hdr.getType()) {
        case OpCode.createSession:                      // 若是 createSession, 则在 sessions 中放入数据
            sessions.put(hdr.getClientId(),
                    ((CreateSessionTxn) txn).getTimeOut());
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG,ZooTrace.SESSION_TRACE_MASK,
                        "playLog --- create session in log: 0x"
                                + Long.toHexString(hdr.getClientId())
                                + " with timeout: "
                                + ((CreateSessionTxn) txn).getTimeOut());
            }
            // give dataTree a chance to sync its lastProcessedZxid
            rc = dt.processTxn(hdr, txn);               // DataTree 处理事务
            break;
        case OpCode.closeSession:
            sessions.remove(hdr.getClientId());        // 移除 session 信息
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG,ZooTrace.SESSION_TRACE_MASK,
                        "playLog --- close session in log: 0x"
                                + Long.toHexString(hdr.getClientId()));
            }
            rc = dt.processTxn(hdr, txn);               // 执行事务
            break;
        default:
            rc = dt.processTxn(hdr, txn);
        }

        /**
         * Snapshots are lazily created. So when a snapshot is in progress,
         * there is a chance for later transactions to make into the
         * snapshot. Then when the snapshot is restored, NONODE/NODEEXISTS
         * errors could occur. It should be safe to ignore these.
         */
        if (rc.err != Code.OK.intValue()) {
            LOG.debug("Ignoring processTxn failure hdr:" + hdr.getType()
                    + ", error: " + rc.err + ", path: " + rc.path);
        }
    }

    /**
     * the last logged zxid on the transaction logs
     * @return the last logged zxid
     */
    public long getLastLoggedZxid() {
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        return txnLog.getLastLoggedZxid();
    }

    /**
     * save the datatree and the sessions into a snapshot
     * @param dataTree the datatree to be serialized onto disk
     * @param sessionsWithTimeouts the sesssion timeouts to be
     * serialized onto disk
     * @throws IOException
     */
    // 将 DataTree 及 sessionsWithTimeouts 刷新到 snapshot 文件里面
    public void save(DataTree dataTree, ConcurrentHashMap<Long, Integer> sessionsWithTimeouts) throws IOException {
        long lastZxid = dataTree.lastProcessedZxid;
        File snapshotFile = new File(snapDir, Util.makeSnapshotName(lastZxid));     // 这时的 snapshot 文件 的名字中以 lastProcessedZxid 来做后缀
        LOG.info("Snapshotting: 0x{} to {}", Long.toHexString(lastZxid), snapshotFile);
        snapLog.serialize(dataTree, sessionsWithTimeouts, snapshotFile);
        
    }

    /**
     * truncate the transaction logs the zxid
     * specified
     * @param zxid the zxid to truncate the logs to
     * @return true if able to truncate the log, false if not
     * @throws IOException
     */
    // 将 txn log 里面 大于 zxid 的事务信息进行删除(详情见 FileTxnLog.truncate())
    public boolean truncateLog(long zxid) throws IOException {
        // close the existing txnLog and snapLog
        close();

        // truncate it
        FileTxnLog truncLog = new FileTxnLog(dataDir);
        boolean truncated = truncLog.truncate(zxid);
        truncLog.close();

        // re-open the txnLog and snapLog
        // I'd rather just close/reopen this object itself, however that 
        // would have a big impact outside ZKDatabase as there are other
        // objects holding a reference to this object.
        txnLog = new FileTxnLog(dataDir);
        snapLog = new FileSnap(snapDir);

        return truncated;
    }
    
    /**
     * the most recent snapshot in the snapshot
     * directory
     * @return the file that contains the most 
     * recent snapshot
     * @throws IOException
     */
    // 找到最新的一个 snapshot 文件
    public File findMostRecentSnapshot() throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);
        return snaplog.findMostRecentSnapshot();
    }
    
    /**
     * the n most recent snapshots
     * @param n the number of recent snapshots
     * @return the list of n most recent snapshots, with
     * the most recent in front
     * @throws IOException
     */
    // 根据 zxid 进行排序, 找到最近的 n 个 snapshot 文件
    public List<File> findNRecentSnapshots(int n) throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);
        return snaplog.findNRecentSnapshots(n);
    }

    /**
     * get the snapshot logs that are greater than
     * the given zxid 
     * @param zxid the zxid that contains logs greater than 
     * zxid
     * @return
     */
    // 获取所有大于 zxid 的 txn log 文件, 并且按 zxid 升续排序
    public File[] getSnapshotLogs(long zxid) {
        return FileTxnLog.getLogFiles(dataDir.listFiles(), zxid);
    }

    /**
     * append the request to the transaction logs
     * @param si the request to be appended
     * returns true iff something appended, otw false 
     * @throws IOException
     */
    // 最佳请求信息到 txnLog 文件,
    public boolean append(Request si) throws IOException {
        return txnLog.append(si.hdr, si.txn);
    }

    /**
     * commit the transaction of logs
     * @throws IOException
     */
    // 将 txn log 日志进行提交
    public void commit() throws IOException {
        txnLog.commit();
    }

    /**
     * roll the transaction logs
     * @throws IOException 
     */
    // 通过调用 rolllog 将 logStream == null, 下次ZKDatabase 进行提交数据时, 会判断 logStream 是否是 null, 来决定是否新创建新的文件s
    public void rollLog() throws IOException {          // 事务日志文件重新生成, 这里将 logStream == null 来判断是否需要重新生成 新的文件
        txnLog.rollLog();
    }
    
    /**
     * close the transaction log files
     * @throws IOException
     */
    public void close() throws IOException {
        txnLog.close();
        snapLog.close();
    }
}

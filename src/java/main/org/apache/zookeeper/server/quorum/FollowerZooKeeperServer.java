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

package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.jute.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * Just like the standard ZooKeeperServer. We just replace the request
 * processors: FollowerRequestProcessor -> CommitProcessor ->
 * FinalRequestProcessor
 * 
 * A SyncRequestProcessor is also spawned off to log proposals from the leader.
 */
public class FollowerZooKeeperServer extends LearnerZooKeeperServer {
    private static final Logger LOG =
        LoggerFactory.getLogger(FollowerZooKeeperServer.class);

    CommitProcessor commitProcessor;

    SyncRequestProcessor syncProcessor;

    /*
     * Pending sync requests
     */
    ConcurrentLinkedQueue<Request> pendingSyncs;
    
    /**
     * @throws IOException
     */
    FollowerZooKeeperServer(FileTxnSnapLog logFactory,QuorumPeer self,
            DataTreeBuilder treeBuilder, ZKDatabase zkDb) throws IOException {
        super(logFactory, self.tickTime, self.minSessionTimeout,
                self.maxSessionTimeout, treeBuilder, zkDb, self);
        this.pendingSyncs = new ConcurrentLinkedQueue<Request>();
    }

    public Follower getFollower(){
        return self.follower;
    }

    /**
     * Follower 的 RequestProcessor 处理链 (2条)
     * 第一条 链
     * FollowerRequestProcessor: 区分处理 Request, 将 Request 交由下个 RequestProcessor, 而若涉及事务的操作, 则 交由 Follower 提交给 leader (zks.getFollower().request())
     * CommitProcessor: 这条链决定这着 Request 能否提交, 里面主要有两条链 , queuedRequests : 存储着 等待 ACK 过半确认的 Request, committedRequests :存储着 已经经过 ACK 过半确认的 Request
     * FinalRequestProcessor: 前面的 Request 只是在经过 SynRequestProcessor 持久化到 txnLog 里面, 而 这里做的就是真正的将数据改变到 ZKDataBase 里面(作为  Follower 一定会在 FollowerZooKeeperServer.logRequest 进行同步Request 数据到磁盘里面后再到 FinalRequestProcessor)
     *
     * 第二条 链
     * SynRequestProcessor: 主要是将 Request 持久化到 TxnLog 里面, 其中涉及到 TxnLog 的滚动, 及 Snapshot 文件的生成
     * AckRequestProcessor: 主要完成 针对 Request 的 ACK 回复, 对 在Leader中就是完成 leader 自己提交 Request, 自己回复 ACK
     *
     * 1. FollowerRequestProcessor --> CommitProcessor --> FinalRequestProcessor
     * 2. SyncRequestProcessor --> SendAckRequestProcessor
     */
    @Override
    protected void setupRequestProcessors() {
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        commitProcessor = new CommitProcessor(finalProcessor, Long.toString(getServerId()), true);
        commitProcessor.start();
        firstProcessor = new FollowerRequestProcessor(this, commitProcessor);
        ((FollowerRequestProcessor) firstProcessor).start();
        syncProcessor = new SyncRequestProcessor(this, new SendAckRequestProcessor((Learner)getFollower()));
        syncProcessor.start();
    }

    LinkedBlockingQueue<Request> pendingTxns = new LinkedBlockingQueue<Request>();

    // 在 Follower 的 processPacket 中进行处理
    public void logRequest(TxnHeader hdr, Record txn) {                          // 接收 Leader 发来的 Proposal
        LOG.info("hdr:" + hdr + ", txn:" + txn);                                // 构建出 Leader 发来的  Request
        Request request = new Request(null, hdr.getClientId(), hdr.getCxid(), hdr.getType(), null, null);
        request.hdr = hdr;
        request.txn = txn;
        request.zxid = hdr.getZxid();
        if ((request.zxid & 0xffffffffL) != 0) {
            pendingTxns.add(request);                                           // 当 request commit 时, 就进行 remove 掉
        }
        syncProcessor.processRequest(request);                                  // 将 Request 交给 SyncRequestProcessor 来进行落磁盘处理, 在落好磁盘后, 调用 SendAckRequestProcessor 进行回复 Leader ACK, 说明 Follower 对于这个 Proposal 已经认可
    }

    /**
     * When a COMMIT message is received, eventually this method is called, 
     * which matches up the zxid from the COMMIT with (hopefully) the head of
     * the pendingTxns queue and hands it to the commitProcessor to commit.
     * @param zxid - must correspond to the head of pendingTxns if it exists
     */
    public void commit(long zxid) {
        if (pendingTxns.size() == 0) {
            LOG.warn("Committing " + Long.toHexString(zxid)
                    + " without seeing txn");
            return;
        }
        long firstElementZxid = pendingTxns.element().zxid;                 // http://blog.csdn.net/fei33423/article/details/53749138
        if (firstElementZxid != zxid) {                                     // 这里就有经典问题, 在 Leader 端提交了 3 个 Proposal 的信息(comit 1, comit 2, comit 3), 但 follower 在接收到 comit 1 后就接收到 comit 3
            LOG.error("Committing zxid 0x" + Long.toHexString(zxid)         // 则就会打印这里的日志, 并且进行退出
                    + " but next pending txn 0x"
                    + Long.toHexString(firstElementZxid));
            System.exit(12);
        }
        Request request = pendingTxns.remove();
        commitProcessor.commit(request);
    }
    
    synchronized public void sync(){
        if(pendingSyncs.size() ==0){
            LOG.warn("Not expecting a sync.");
            return;
        }
                
        Request r = pendingSyncs.remove();
		commitProcessor.commit(r);
    }
             
    @Override
    public int getGlobalOutstandingLimit() {
        return super.getGlobalOutstandingLimit() / (self.getQuorumSize() - 1);
    }
    
    @Override
    public void shutdown() {
        LOG.info("Shutting down");
        try {
            super.shutdown();
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception during shutdown", e);
        }
        try {
            if (syncProcessor != null) {
                syncProcessor.shutdown();
            }
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception in syncprocessor shutdown",
                    e);
        }
    }
    
    @Override
    public String getState() {
        return "follower";
    }

    @Override
    public Learner getLearner() {
        return getFollower();
    }
}

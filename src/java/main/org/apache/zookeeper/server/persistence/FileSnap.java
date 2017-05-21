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
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.util.SerializeUtils;

/**
 * This class implements the snapshot interface.
 * it is responsible for storing, serializing
 * and deserializing the right snapshot.
 * and provides access to the snapshots.
 */
public class FileSnap implements SnapShot {

    File snapDir;
    private volatile boolean close = false;
    private static final int VERSION=2;
    private static final long dbId=-1;
    private static final Logger LOG = LoggerFactory.getLogger(FileSnap.class);
    public final static int SNAP_MAGIC
            = ByteBuffer.wrap("ZKSN".getBytes()).getInt();
    public FileSnap(File snapDir) {
        this.snapDir = snapDir;
    }

    /**
     * deserialize a data tree from the most recent snapshot
     *
     * 反序列化 (从最近的 snapshot 里面恢复出来 DataTree)
     * @return the zxid of the snapshot
     */
    public long deserialize(DataTree dt, Map<Long, Integer> sessions) throws IOException {
        // we run through 100 snapshots (not all of them)
        // if we cannot get it running within 100 snapshots
        // we should  give up

        List<File> snapList = findNValidSnapshots(100);                         // 获取最新的 100 个 snapshot 文件, 文件是按其对应的名称排序来返回, 最新的文件在最上面
        if (snapList.size() == 0) {
            return -1L;
        }
        File snap = null;
        boolean foundValid = false;
        for (int i = 0; i < snapList.size(); i++) {                             // 这里会轮询上面获取的最新的 100 个文件, 从最新的一个开始处理, 只要有一个处理成功, 就直接退出
            snap = snapList.get(i);
            InputStream snapIS = null;
            CheckedInputStream crcIn = null;
            try {
                LOG.info("Reading snapshot " + snap);
                snapIS = new BufferedInputStream(new FileInputStream(snap));
                crcIn = new CheckedInputStream(snapIS, new Adler32());          // 通过 Adler32 算法获取 checkSum
                InputArchive ia = BinaryInputArchive.getArchive(crcIn);
                deserialize(dt,sessions, ia);                                   // 从数据流反序列化出 DataTree 与 sessions
                long checkSum = crcIn.getChecksum().getValue();                 // 获取文件对应的 checkSum ()
                long val = ia.readLong("val");                                  // 对应的 checkSum 是一个 long 类型, 写在了文件的最后部分
                if (val != checkSum) {                                          // 校验通过 Adler32 算法获取的 checkSum 与文件中存储的 checkSum 进行比较 ( 让我想起个项目 Jafka, 通过 CRC32 来进行数据可行性的校验)
                    throw new IOException("CRC corruption in snapshot :  " + snap);
                }
                foundValid = true;
                break;                                                          // 从这个 break 可以看出, 只要在检测出有一个 snapshot文件有效, 则就退出
            } catch(IOException e) {
                LOG.warn("problem reading snap file " + snap, e);
            } finally {
                if (snapIS != null)                                             // 关闭对应数据流
                    snapIS.close();
                if (crcIn != null)
                    crcIn.close();
            }
        }

        if (!foundValid) {      // 没有找到有效的 snapshot 文件
            throw new IOException("Not able to find valid snapshots in " + snapDir);
        }
        dt.lastProcessedZxid = Util.getZxidFromName(snap.getName(), "snapshot"); // snapshot 的名字中包含里面存储的 Txn 的最大的 zxid
        return dt.lastProcessedZxid;                                            // 返回 snapshot 中最新的 zxid(后期还要加载大于这个 zxid 的事务信息)
    }

    /**
     * deserialize the datatree from an inputarchive
     * @param dt the datatree to be serialized into
     * @param sessions the sessions to be filled up
     * @param ia the input archive to restore from
     * @throws IOException
     */
    // 从数据流 ia 中反序列化出 文件头(FileHeader)
    public void deserialize(DataTree dt, Map<Long, Integer> sessions,
                            InputArchive ia) throws IOException {
        FileHeader header = new FileHeader();
        header.deserialize(ia, "fileheader");                   // 从 数据流中反序列化出 FileHeader, 并且校验对应的 SNAP_MAGIC 是否和写入时一样, 这是校验文件有没有破坏
        if (header.getMagic() != SNAP_MAGIC) {
            throw new IOException("mismatching magic headers "
                    + header.getMagic() +
                    " !=  " + FileSnap.SNAP_MAGIC);
        }
        SerializeUtils.deserializeSnapshot(dt,ia,sessions);     // 从数据流中反序列化出 DataTree, sessions
    }

    /**
     * find the most recent snapshot in the database.
     * @return the file containing the most recent snapshot
     */
    public File findMostRecentSnapshot() throws IOException {   // 获取最新的一个 snapshot 文件
        List<File> files = findNValidSnapshots(1);
        if (files.size() == 0) {
            return null;
        }
        return files.get(0);
    }

    /**
     * find the last (maybe) valid n snapshots. this does some 
     * minor checks on the validity of the snapshots. It just
     * checks for / at the end of the snapshot. This does
     * not mean that the snapshot is truly valid but is
     * valid with a high probability. also, the most recent 
     * will be first on the list. 
     * @param n the number of most recent snapshots
     * @return the last n snapshots (the number might be
     * less than n in case enough snapshots are not available).
     * @throws IOException
     */
    // 获取 最新的 n 个 snapshot 文件, 并且通过校验文件的末尾是否是 / 来确定文件是否是有效
    private List<File> findNValidSnapshots(int n) throws IOException {
        // 根据文件的名称来进行排序 文件
        List<File> files = Util.sortDataDir(snapDir.listFiles(),"snapshot", false);
        int count = 0;
        List<File> list = new ArrayList<File>();
        for (File f : files) {
            // we should catch the exceptions
            // from the valid snapshot and continue
            // until we find a valid one
            try {
                if (Util.isValidSnapshot(f)) { // 通过文件大小及文件最后5(1 与 '/')个字节来判断是否是有效的文件
                    list.add(f);
                    count++;
                    if (count == n) {
                        break;
                    }
                }
            } catch (IOException e) {
                LOG.info("invalid snapshot " + f, e);
            }
        }
        return list;
    }

    /**
     * find the last n snapshots. this does not have
     * any checks if the snapshot might be valid or not
     * @param the number of most recent snapshots 
     * @return the last n snapshots
     * @throws IOException
     */
    public List<File> findNRecentSnapshots(int n) throws IOException {
        List<File> files = Util.sortDataDir(snapDir.listFiles(), "snapshot", false);    // 通过 snapshotFile 里面含有的 zxid 的文件名来进行 snapshotFile 排序
        int i = 0;
        List<File> list = new ArrayList<File>();
        for (File f: files) {                                                           // 循环 snapDirFile 获取最新的 n 个文件
            if (i==n)
                break;
            i++;
            list.add(f);
        }
        return list;                                                                    // 进行 文件的返回
    }

    /**
     * serialize the datatree and sessions
     * @param dt the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param oa the output archive to serialize into
     * @param header the header of this snapshot
     * @throws IOException
     */
    // 序列化 DataTree 与 sessions 到 snapshot 文件上
    protected void serialize(DataTree dt,Map<Long, Integer> sessions, OutputArchive oa, FileHeader header) throws IOException {
        // this is really a programmatic error and not something that can
        // happen at runtime
        if(header==null) throw new IllegalStateException("Snapshot's not open for writing: uninitialized header");

        header.serialize(oa, "fileheader");                 // 文件头进行序列化
        SerializeUtils.serializeSnapshot(dt,oa,sessions);   // 序列化 DataTree, sessions 序列化到数据流中
    }

    /**
     * serialize the datatree and session into the file snapshot
     * @param dt the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param snapShot the file to store snapshot into
     *
     * 序列化 DataTree 与 sessions 到 snapshot 文件上
     */
    public synchronized void serialize(DataTree dt, Map<Long, Integer> sessions, File snapShot) throws IOException {
        if (!close) {
            OutputStream sessOS = new BufferedOutputStream(new FileOutputStream(snapShot));
            CheckedOutputStream crcOut = new CheckedOutputStream(sessOS, new Adler32());
            //CheckedOutputStream cout = new CheckedOutputStream()
            OutputArchive oa = BinaryOutputArchive.getArchive(crcOut);                      // 封装出对应的数据流
            FileHeader header = new FileHeader(SNAP_MAGIC, VERSION, dbId);                  // snapshot 的文件头
            serialize(dt,sessions,oa, header);                                              // 将 DataTree, sessions, FileHeader 序列化到 snapshot 文件里面
            long val = crcOut.getChecksum().getValue();                                     // 通过 Adler32 算法来获取对应数据流的 checkSum, 用于校验数据的可信性
            oa.writeLong(val, "val");                                                       // 这个是检测文件完整性的校验值
            oa.writeString("/", "path");                                                    // 因为在进行 take snap shot 时, 可能进程被 kill, 所以用于校验文件的完整性
            sessOS.flush();
            crcOut.close();
            sessOS.close();
        }
    }

    /**
     * synchronized close just so that if serialize is in place
     * the close operation will block and will wait till serialize
     * is done and will set the close flag
     */
    @Override
    public synchronized void close() throws IOException {
        close = true;
    }

}

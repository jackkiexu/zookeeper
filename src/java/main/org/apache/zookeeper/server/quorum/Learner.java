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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * 参考资料
 * http://blog.csdn.net/vinowan/article/details/22196897
 *
 * This class is the superclass of two of the three main actors in a ZK
 * ensemble: Followers and Observers. Both Followers and Observers share 
 * a good deal of code which is moved into Peer to avoid duplication. 
 */
public class Learner {       
    static class PacketInFlight {
        TxnHeader hdr;
        Record rec;

        @Override
        public String toString() {
            return "PacketInFlight{" +
                    "hdr=" + hdr +
                    ", rec=" + rec +
                    '}';
        }
    }
    QuorumPeer self;
    LearnerZooKeeperServer zk;
    
    protected BufferedOutputStream bufferedOutput;
    
    protected Socket sock;
    
    /**
     * Socket getter
     * @return 
     */
    public Socket getSocket() {
        return sock;
    }
    
    protected InputArchive leaderIs;
    protected OutputArchive leaderOs;  
    /** the protocol version of the leader */
    protected int leaderProtocolVersion = 0x01;
    
    protected static final Logger LOG = LoggerFactory.getLogger(Learner.class);

    static final private boolean nodelay = System.getProperty("follower.nodelay", "true").equals("true");
    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }   
    
    final ConcurrentHashMap<Long, ServerCnxn> pendingRevalidations =
        new ConcurrentHashMap<Long, ServerCnxn>();
    
    public int getPendingRevalidationsCount() {
        return pendingRevalidations.size();
    }
    
    /**
     * validate a session for a client
     *
     * @param clientId
     *                the client to be revalidated
     * @param timeout
     *                the timeout for which the session is valid
     * @return
     * @throws IOException
     */
    void validateSession(ServerCnxn cnxn, long clientId, int timeout)
            throws IOException {
        LOG.info("Revalidating client: 0x" + Long.toHexString(clientId));
        LOG.info("validateSession :" + cnxn + ", clientId:" + clientId + ", timeout:" + timeout);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(clientId);
        dos.writeInt(timeout);
        dos.close();
        QuorumPacket qp = new QuorumPacket(Leader.REVALIDATE, -1, baos.toByteArray(), null);
        pendingRevalidations.put(clientId, cnxn);
        ZooTrace.logTraceMessage(LOG,
                ZooTrace.SESSION_TRACE_MASK,
                "To validate session 0x"
                        + Long.toHexString(clientId));
        writePacket(qp, true);
    }     
    
    /**
     * write a packet to the leader
     *
     * @param pp
     *                the proposal packet to be sent to the leader
     * @throws IOException
     */
    // 将 QuorumPeer 之间的数据包发送给 Leader
    void writePacket(QuorumPacket pp, boolean flush) throws IOException {
        synchronized (leaderOs) {
            if (pp != null) {
                leaderOs.writeRecord(pp, "packet");
            }
            if (flush) {
                bufferedOutput.flush();
            }
        }
    }

    /**
     * read a packet from the leader
     *
     * @param pp
     *                the packet to be instantiated
     * @throws IOException
     */
    void readPacket(QuorumPacket pp) throws IOException {       // 从 leader 读取发送过来的数据包
        synchronized (leaderIs) {
            leaderIs.readRecord(pp, "packet"); // 从 leaderIs 里面读取出 QuorumPacket 的值
        }
        LOG.info("pp:" + pp);
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        if (pp.getType() == Leader.PING) {
            traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logQuorumPacket(LOG, traceMask, 'i', pp);
        }
    }
    
    /**
     * send a request packet to the leader
     *
     * @param request
     *                the request from the client
     * @throws IOException
     */
    void request(Request request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();       // 将要发送给 Leader 的数据包序列化
        DataOutputStream oa = new DataOutputStream(baos);
        oa.writeLong(request.sessionId);
        oa.writeInt(request.cxid);
        oa.writeInt(request.type);
        if (request.request != null) {
            request.request.rewind();
            int len = request.request.remaining();
            byte b[] = new byte[len];
            request.request.get(b);
            request.request.rewind();
            oa.write(b);
        }
        oa.close();                                                     // 封装请求数据包
        QuorumPacket qp = new QuorumPacket(Leader.REQUEST, -1, baos.toByteArray(), request.authInfo);

        writePacket(qp, true);                                         // 将 事务请求 request 发送给 Leader
    }
    
    /**
     * Returns the address of the node we think is the leader.
     */
    // 返回 Leader 的网络信息
    protected InetSocketAddress findLeader() {
        InetSocketAddress addr = null;
        // Find the leader by id
        Vote current = self.getCurrentVote();            // 获取 QuorumPeer 的投票信息, 里面包含自己Leader选举所投的信息
        for (QuorumServer s : self.getView().values()) {
            if (s.id == current.getId()) {
                addr = s.addr;                          // 获取 Leader 的 addr
                break;
            }
        }
        if (addr == null) {
            LOG.warn("Couldn't find the leader with id = "
                    + current.getId());
        }
        return addr;
    }
    
    /**
     * Establish a connection with the Leader found by findLeader. Retries
     * 5 times before giving up. 
     * @param addr - the address of the Leader to connect to.
     * @throws IOException - if the socket connection fails on the 5th attempt
     * @throws ConnectException
     * @throws InterruptedException
     */
    // 连接 leader, 建立成功后, 在 Leader 端会有一个 LearnerHandler 处理与之的通信
    protected void connectToLeader(InetSocketAddress addr) throws IOException, ConnectException, InterruptedException {

        sock = new Socket();        
        sock.setSoTimeout(self.tickTime * self.initLimit);          // 这里的 SoTimeout 很重要, 若 InputStream.read 超过这个时间,则会报出 SocketTimeoutException 异常
        for (int tries = 0; tries < 5; tries++) {                   // 连接 Leader 尝试 5次, 若还是失败, 则抛出异常, 一直往外抛出, 直到 QuorumPeer 的重新开始选举 leader run 方法里面 -> 进行选举 Leader
            try {
                sock.connect(addr, self.tickTime * self.syncLimit); // 连接 leader
                sock.setTcpNoDelay(nodelay);                        // 设置 tcpnoDelay <- 这里其实就是禁止 tcp 底层合并小数据包, 一次发送所有数据的 算法
                break;
            } catch (IOException e) {
                if (tries == 4) {
                    LOG.error("Unexpected exception",e);
                    throw e;
                } else {
                    LOG.warn("Unexpected exception, tries="+tries+
                            ", connecting to " + addr,e);
                    sock = new Socket();
                    sock.setSoTimeout(self.tickTime * self.initLimit);
                }
            }
            Thread.sleep(1000);
        }
        leaderIs = BinaryInputArchive.getArchive(new BufferedInputStream(sock.getInputStream()));   // 封装对应的 I/O 数据流
        bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
        leaderOs = BinaryOutputArchive.getArchive(bufferedOutput);                                  // 封装输出数据流
    }   
    
    /**
     * Once connected to the leader, perform the handshake protocol to
     * establish a following / observing connection. 
     * @param pktType
     * @return the zxid the Leader sends for synchronization purposes.
     * @throws IOException
     */
    protected long registerWithLeader(int pktType) throws IOException{

        LOG.info("registerWithLeader:" + pktType);
        /*
         * Send follower info, including last zxid and sid
         */
    	long lastLoggedZxid = self.getLastLoggedZxid();                     // 获取 Follower 的最后处理的 zxid
        QuorumPacket qp = new QuorumPacket();                
        qp.setType(pktType);                                                // 若是 Follower ,则当前的角色是  Leader.FOLLOWERINFO
        qp.setZxid(ZxidUtils.makeZxid(self.getAcceptedEpoch(), 0));         // Follower 的 lastZxid 的值
        
        /*
         * Add sid to payload
         */
        LearnerInfo li = new LearnerInfo(self.getId(), 0x10000);            // 将 Follower 的信息封装成 LearnerInfo
        LOG.info("li:" + li);

        ByteArrayOutputStream bsid = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(bsid);
        boa.writeRecord(li, "LearnerInfo");
        qp.setData(bsid.toByteArray());                                     // 在 QuorumPacket 里面添加 Follower 的信息
        LOG.info("qp:" + qp);
        
        writePacket(qp, true);                                              // 发送 QuorumPacket 包括 learnerInfo 与 Leader.FOLLOWERINFO, 通过 self.getAcceptedEpoch() 构成的 zxid
        readPacket(qp);                                                     // 读取 leader 返回的数据 (这里读取的数据包是 Leader 的数据信息 (LEADERINFO) )

        LOG.info("qp:" + qp);
        final long newEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());

        LOG.info("newEpoch:" + newEpoch);
		if (qp.getType() == Leader.LEADERINFO) {                            // 处理 Leader 发来的 leader 信息 (里面包括 集群中 Leader 选举的 epoch 值)
        	// we are connected to a 1.0 server so accept the new epoch and read the next packet
        	leaderProtocolVersion = ByteBuffer.wrap(qp.getData()).getInt();
            LOG.info("leaderProtocolVersion:" + leaderProtocolVersion);
        	byte epochBytes[] = new byte[4];
        	final ByteBuffer wrappedEpochBytes = ByteBuffer.wrap(epochBytes);

            LOG.info("newEpoch:" + newEpoch + ", self.getAcceptedEpoch():" + self.getAcceptedEpoch());
        	if (newEpoch > self.getAcceptedEpoch()) {                       // 若 Follower 的 election 的 epoch 值小于自己, 则用 Leader 的
        		wrappedEpochBytes.putInt((int)self.getCurrentEpoch());
        		self.setAcceptedEpoch(newEpoch);
        	} else if (newEpoch == self.getAcceptedEpoch()) {
        		// since we have already acked an epoch equal to the leaders, we cannot ack
        		// again, but we still need to send our lastZxid to the leader so that we can
        		// sync with it if it does assume leadership of the epoch.
        		// the -1 indicates that this reply should not count as an ack for the new epoch
                wrappedEpochBytes.putInt(-1);
        	} else {                                                         // 若 Follower.epoch > Leader.epoch 则说明前面的 Leader 选举出错了
        		throw new IOException("Leaders epoch, " + newEpoch + " is less than accepted epoch, " + self.getAcceptedEpoch());
        	}                                                                // 在 接收到 Leader.LEADERINFO 的消息后, 进行回复 Leader.ACKEPOCH 的消息, 并且加上 lastLargestZxid 值
        	QuorumPacket ackNewEpoch = new QuorumPacket(Leader.ACKEPOCH, lastLoggedZxid, epochBytes, null);

            LOG.info("ackNewEpoch:" + ackNewEpoch);
        	writePacket(ackNewEpoch, true);                                  // 将 ACKEPOCH 信息发送给对方 用于回复Leader发过来的LEADERINFO
            return ZxidUtils.makeZxid(newEpoch, 0);
        } else {
            LOG.info("newEpoch :" + newEpoch + ", self.getAcceptedEpoch():" + self.getAcceptedEpoch());
        	if (newEpoch > self.getAcceptedEpoch()) {
        		self.setAcceptedEpoch(newEpoch);
        	}
            if (qp.getType() != Leader.NEWLEADER) {
                LOG.error("First packet should have been NEWLEADER");
                throw new IOException("First packet should have been NEWLEADER");
            }
            return qp.getZxid();
        }
    } 
    
    /**
     * Finally, synchronize our history with the Leader. 
     * @param newLeaderZxid
     * @throws IOException
     * @throws InterruptedException
     */
    // 根据 InputStream 里面的数据流 来进行同步数据
    protected void syncWithLeader(long newLeaderZxid) throws IOException, InterruptedException{
        LOG.info("syncWithLeader:" + newLeaderZxid);
        QuorumPacket ack = new QuorumPacket(Leader.ACK, 0, null, null);
        QuorumPacket qp = new QuorumPacket();
        long newEpoch = ZxidUtils.getEpochFromZxid(newLeaderZxid);
        
        readPacket(qp);                                                       // 从 对应的 InputStream 读取出一个 QuorumPacket
        LinkedList<Long> packetsCommitted = new LinkedList<Long>();
        LinkedList<PacketInFlight> packetsNotCommitted = new LinkedList<PacketInFlight>();
        LOG.info("qp:" + qp);

        synchronized (zk) {
            if (qp.getType() == Leader.DIFF) {                              // DIFF 数据包
                LOG.info("Getting a diff from the leader 0x" + Long.toHexString(qp.getZxid()));                
            }
            else if (qp.getType() == Leader.SNAP) {                         // 收到的信息是 snap, 则从 leader 复制一份 镜像数据到本地
                LOG.info("Getting a snapshot from leader");
                // The leader is going to dump the database
                // clear our own database and read
                zk.getZKDatabase().clear();
                zk.getZKDatabase().deserializeSnapshot(leaderIs);           // 从 InputStream 里面 反序列化出 DataTree
                String signature = leaderIs.readString("signature");        // 看了一个 读取 tag "signature" 代表的一个 String 对象
                if (!signature.equals("BenWasHere")) {
                    LOG.error("Missing signature. Got " + signature);
                    throw new IOException("Missing signature");                   
                }
            } else if (qp.getType() == Leader.TRUNC) {                     // 回滚到对应的事务
                //we need to truncate the log to the lastzxid of the leader
                LOG.warn("Truncating log to get in sync with the leader 0x" + Long.toHexString(qp.getZxid()));
                boolean truncated=zk.getZKDatabase().truncateLog(qp.getZxid());
                LOG.info("truncated:" + truncated + ", qp.getZxid():" + qp.getZxid());
                if (!truncated) {
                    // not able to truncate the log
                    LOG.error("Not able to truncate the log "
                            + Long.toHexString(qp.getZxid()));
                    System.exit(13);
                }

            }
            else {
                LOG.error("Got unexpected packet from leader "
                        + qp.getType() + " exiting ... " );
                System.exit(13);

            }
            zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());          // 因为这里的 ZKDatatree 是从 Leader 的 SnapShot 的 InputStream 里面获取的, 所以调用这里通过 set 进行赋值
            zk.createSessionTracker();                                      // Learner 创建对应的 SessionTracker (Follower/Observer)
            
            long lastQueued = 0;

            // in V1.0 we take a snapshot when we get the NEWLEADER message, but in pre V1.0
            // we take the snapshot at the UPDATE, since V1.0 also gets the UPDATE (after the NEWLEADER)
            // we need to make sure that we don't take the snapshot twice.
            boolean snapshotTaken = false;
            // we are now going to start getting transactions to apply followed by an UPTODATE
            outerLoop:
            while (self.isRunning()) {                                     // 同步完数据后, 准备执行投票 这里的 self.isRunning() 默认就是 true
                readPacket(qp);

                LOG.info("qp:" + qp);

                switch(qp.getType()) {
                case Leader.PROPOSAL:                                     // 将投票信息加入到 待处理列表
                    PacketInFlight pif = new PacketInFlight();
                    pif.hdr = new TxnHeader();
                    pif.rec = SerializeUtils.deserializeTxn(qp.getData(), pif.hdr);         // 反序列化对应的 请求事务体
                    LOG.info("pif:" + pif);
                    if (pif.hdr.getZxid() != lastQueued + 1) {
                    LOG.warn("Got zxid 0x"
                            + Long.toHexString(pif.hdr.getZxid())
                            + " expected 0x"
                            + Long.toHexString(lastQueued + 1));
                    }
                    lastQueued = pif.hdr.getZxid();
                    packetsNotCommitted.add(pif);
                    break;
                case Leader.COMMIT:                                        // commit 则将事务提交给 Server 处理
                    LOG.info("snapshotTaken :" + snapshotTaken);
                    if (!snapshotTaken) {
                        pif = packetsNotCommitted.peekFirst();
                        if (pif.hdr.getZxid() != qp.getZxid()) {
                            LOG.warn("Committing " + qp.getZxid() + ", but next proposal is " + pif.hdr.getZxid());
                        } else {
                            zk.processTxn(pif.hdr, pif.rec);               // 处理对应的事件
                            packetsNotCommitted.remove();
                        }
                    } else {
                        packetsCommitted.add(qp.getZxid());
                    }
                    break;
                case Leader.INFORM:                                                         // 这个 INFORM 只有Observer 才会处理
                    /*
                     * Only observer get this type of packet. We treat this
                     * as receiving PROPOSAL and COMMMIT.
                     */
                    PacketInFlight packet = new PacketInFlight();
                    packet.hdr = new TxnHeader();
                    packet.rec = SerializeUtils.deserializeTxn(qp.getData(), packet.hdr);
                    LOG.info("packet:" + packet);
                    // Log warning message if txn comes out-of-order
                    if (packet.hdr.getZxid() != lastQueued + 1) {
                        LOG.warn("Got zxid 0x"
                                + Long.toHexString(packet.hdr.getZxid())
                                + " expected 0x"
                                + Long.toHexString(lastQueued + 1));
                    }
                    lastQueued = packet.hdr.getZxid();
                    LOG.info("snapshotTaken : " + snapshotTaken);
                    if (!snapshotTaken) {
                        // Apply to db directly if we haven't taken the snapshot
                        zk.processTxn(packet.hdr, packet.rec);
                    } else {
                        packetsNotCommitted.add(packet);
                        packetsCommitted.add(qp.getZxid());
                    }
                    break;
                case Leader.UPTODATE:                                               // UPTODATE 数据包, 说明同步数据成功, 退出循环
                    LOG.info("snapshotTaken : " + snapshotTaken + ", newEpoch:" + newEpoch);
                    if (!snapshotTaken) { // true for the pre v1.0 case
                        zk.takeSnapshot();
                        self.setCurrentEpoch(newEpoch);
                    }
                    self.cnxnFactory.setZooKeeperServer(zk);                
                    break outerLoop;                                                // 获取 UPTODATE 后 退出 while loop
                case Leader.NEWLEADER: // it will be NEWLEADER in v1.0              // 说明之前残留的投票已经处理完, 则将内存中的数据写入文件, 并发送 ACK 包
                    LOG.info("newEpoch:" + newEpoch);
                    // Create updatingEpoch file and remove it after current
                    // epoch is set. QuorumPeer.loadDataBase() uses this file to
                    // detect the case where the server was terminated after
                    // taking a snapshot but before setting the current epoch.
                    File updating = new File(self.getTxnFactory().getSnapDir(),
                                        QuorumPeer.UPDATING_EPOCH_FILENAME);
                    if (!updating.exists() && !updating.createNewFile()) {
                        throw new IOException("Failed to create " +
                                              updating.toString());
                    }
                    zk.takeSnapshot();
                    self.setCurrentEpoch(newEpoch);
                    if (!updating.delete()) {
                        throw new IOException("Failed to delete " +
                                              updating.toString());
                    }
                    snapshotTaken = true;
                    writePacket(new QuorumPacket(Leader.ACK, newLeaderZxid, null, null), true);
                    break;
                }
            }
        }
        ack.setZxid(ZxidUtils.makeZxid(newEpoch, 0));                                           // 更新 zixd newLeaderZxid & ~0xffffffffL, 表明已经切换到新的 epoch

        LOG.info("ack:" + ack);
        writePacket(ack, true);                                                                // 回复 leader ack 消息 (针对 数据包 Leader.UPTODATE )
        sock.setSoTimeout(self.tickTime * self.syncLimit);                                  // 设置 InputStream.read 的超时时间
        zk.startup();                                                                           // 启动 Learner zookeeper server
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         * 
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        self.updateElectionVote(newEpoch);                                                      // 更新最新的 newEpoch

        // We need to log the stuff that came in between the snapshot and the uptodate
        if (zk instanceof FollowerZooKeeperServer) {
            FollowerZooKeeperServer fzk = (FollowerZooKeeperServer)zk;

            LOG.info("packetsNotCommitted:" + packetsNotCommitted);
            for(PacketInFlight p: packetsNotCommitted) {
                fzk.logRequest(p.hdr, p.rec);
            }

            LOG.info("packetsCommitted:" + packetsCommitted);
            for(Long zxid: packetsCommitted) {
                fzk.commit(zxid);
            }
        } else if (zk instanceof ObserverZooKeeperServer) {
            // Similar to follower, we need to log requests between the snapshot
            // and UPTODATE
            ObserverZooKeeperServer ozk = (ObserverZooKeeperServer) zk;

            LOG.info("packetsNotCommitted:" + packetsNotCommitted);
            for (PacketInFlight p : packetsNotCommitted) {
                Long zxid = packetsCommitted.peekFirst();
                if (p.hdr.getZxid() != zxid) {
                    // log warning message if there is no matching commit
                    // old leader send outstanding proposal to observer
                    LOG.warn("Committing " + Long.toHexString(zxid)
                            + ", but next proposal is "
                            + Long.toHexString(p.hdr.getZxid()));
                    continue;
                }
                packetsCommitted.remove();
                Request request = new Request(null, p.hdr.getClientId(),
                        p.hdr.getCxid(), p.hdr.getType(), null, null);
                request.txn = p.rec;
                request.hdr = p.hdr;

                LOG.info("request:" + request);
                ozk.commitRequest(request);
            }
        } else {
            // New server type need to handle in-flight packets
            throw new UnsupportedOperationException("Unknown server type");
        }
    }
    
    protected void revalidate(QuorumPacket qp) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(qp
                .getData());
        DataInputStream dis = new DataInputStream(bis);
        long sessionId = dis.readLong();
        boolean valid = dis.readBoolean();
        ServerCnxn cnxn = pendingRevalidations
        .remove(sessionId);
        if (cnxn == null) {
            LOG.warn("Missing session 0x"
                    + Long.toHexString(sessionId)
                    + " for validation");
        } else {
            zk.finishSessionInit(cnxn, valid);
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                    ZooTrace.SESSION_TRACE_MASK,
                    "Session 0x" + Long.toHexString(sessionId)
                    + " is valid: " + valid);
        }
    }

    // Follower 将自己的 sessionId 及超时时间发送给 Leader, 让 Leader 进行 touch 操作, 校验是否 session 超时
    protected void ping(QuorumPacket qp) throws IOException {
        // Send back the ping with our session data
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        HashMap<Long, Integer> touchTable = zk                          // 获取 Follower/Observer 的 touchTable(sessionId <-> sessionTimeout) 发给 Leader 进行session超时的检测s
                .getTouchSnapshot();
        for (Entry<Long, Integer> entry : touchTable.entrySet()) {
            dos.writeLong(entry.getKey());
            dos.writeInt(entry.getValue());
        }
        qp.setData(bos.toByteArray());                                  // 转化成字节数组, 进行数据的写入
        writePacket(qp, true);                                         // 发送数据包
    }
    
    
    /**
     * Shutdown the Peer
     */
    public void shutdown() {
        // set the zookeeper server to null
        self.cnxnFactory.setZooKeeperServer(null);
        // clear all the connections
        self.cnxnFactory.closeAll();
        // shutdown previous zookeeper
        if (zk != null) {
            zk.shutdown();
        }
    }
}

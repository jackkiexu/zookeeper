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
/**
 * 参考资料
 * http://www.aboutyun.com/thread-10286-1-1.html
 */

package org.apache.zookeeper.server.quorum;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * There will be an instance of this class created by the Leader for each
 * learner. All communication with a learner is handled by this
 * class.
 */
public class LearnerHandler extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(LearnerHandler.class);

    protected final Socket sock;    

    public Socket getSocket() {
        return sock;
    }

    final Leader leader;

    /** Deadline for receiving the next ack. If we are bootstrapping then
     * it's based on the initLimit, if we are done bootstrapping it's based
     * on the syncLimit. Once the deadline is past this learner should
     * be considered no longer "sync'd" with the leader. */
    volatile long tickOfNextAckDeadline;
    
    /**
     * ZooKeeper server identifier of this learner
     */
    protected long sid = 0;
    
    long getSid(){
        return sid;
    }                    

    protected int version = 0x1;
    
    int getVersion() {
    	return version;
    }
    
    /**
     * The packets to be sent to the learner
     */
    final LinkedBlockingQueue<QuorumPacket> queuedPackets =
        new LinkedBlockingQueue<QuorumPacket>();

    /**
     * This class controls the time that the Leader has been
     * waiting for acknowledgement of a proposal from this Learner.
     * If the time is above syncLimit, the connection will be closed.
     * It keeps track of only one proposal at a time, when the ACK for
     * that proposal arrives, it switches to the last proposal received
     * or clears the value if there is no pending proposal.
     */
    private class SyncLimitCheck {
        private boolean started = false;
        private long currentZxid = 0;
        private long currentTime = 0;
        private long nextZxid = 0;
        private long nextTime = 0;

        public synchronized void start() {
            LOG.info("SyncLimitCheck started = true");
            started = true;
        }

        public synchronized void updateProposal(long zxid, long time) {
            LOG.info("zxid :" + zxid + ", time:" + time);
            if (!started) {
                return;
            }
            if (currentTime == 0) {
                currentTime = time;
                currentZxid = zxid;
            } else {
                nextTime = time;
                nextZxid = zxid;
            }
        }

        public synchronized void updateAck(long zxid) {
            LOG.info("currentZxid : " + currentZxid + ", zxid:" + zxid);
             if (currentZxid == zxid) {
                 currentTime = nextTime;
                 currentZxid = nextZxid;
                 nextTime = 0;
                 nextZxid = 0;
             } else if (nextZxid == zxid) {
                 LOG.warn("ACK for " + zxid + " received before ACK for " + currentZxid + "!!!!");
                 nextTime = 0;
                 nextZxid = 0;
             }
        }

        public synchronized boolean check(long time) {
            if (currentTime == 0) {
                return true;
            } else {
                long msDelay = (time - currentTime) / 1000000;
                return (msDelay < (leader.self.tickTime * leader.self.syncLimit));
            }
        }
    };

    private SyncLimitCheck syncLimitCheck = new SyncLimitCheck();

    private BinaryInputArchive ia;

    private BinaryOutputArchive oa;

    private BufferedOutputStream bufferedOutput;

    LearnerHandler(Socket sock, Leader leader) throws IOException {
        super("LearnerHandler-" + sock.getRemoteSocketAddress());
        this.sock = sock;
        this.leader = leader;
        leader.addLearnerHandler(this);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LearnerHandler ").append(sock);
        sb.append(" tickOfNextAckDeadline:").append(tickOfNextAckDeadline());
        sb.append(" synced?:").append(synced());
        sb.append(" queuedPacketLength:").append(queuedPackets.size());
        return sb.toString();
    }

    /**
     * If this packet is queued, the sender thread will exit
     */
    final QuorumPacket proposalOfDeath = new QuorumPacket();

    private LearnerType  learnerType = LearnerType.PARTICIPANT;
    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * This method will use the thread to send packets added to the
     * queuedPackets list
     *
     * @throws InterruptedException
     */
    private void sendPackets() throws InterruptedException {
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        while (true) {
            try {
                QuorumPacket p;
                p = queuedPackets.poll();                                       // 将 Leader 发送给 Follower 的 Request 取出来
                LOG.info(" p :" + p);
                if (p == null) {
                    bufferedOutput.flush();
                    p = queuedPackets.take();                                   // 取出数据为空, 程序在这边阻塞着
                }
                LOG.info("p == proposalOfDeath :" + (p == proposalOfDeath));
                if (p == proposalOfDeath) {                                    // 若取出的数据包是 proposalOfDeath, 则退出 while loop
                    // Packet of death!
                    break;
                }
                LOG.info("p.getType() : " + p);
                if (p.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (p.getType() == Leader.PROPOSAL) {
                    syncLimitCheck.updateProposal(p.getZxid(), System.nanoTime());
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'o', p);
                }
                oa.writeRecord(p, "packet");                                    // 将数据包写到远端
            } catch (IOException e) {
                if (!sock.isClosed()) {
                    LOG.warn("Unexpected exception at " + this, e);
                    try {
                        // this will cause everything to shutdown on
                        // this learner handler and will help notify
                        // the learner/observer instantaneously
                        sock.close();
                    } catch(IOException ie) {
                        LOG.warn("Error closing socket for handler " + this, ie);
                    }
                }
                break;
            }
        }
    }

    static public String packetToString(QuorumPacket p) {
        if (true)
            return null;
        String type = null;
        String mess = null;
        Record txn = null;
        
        switch (p.getType()) {
        case Leader.ACK:
            type = "ACK";
            break;
        case Leader.COMMIT:
            type = "COMMIT";
            break;
        case Leader.FOLLOWERINFO:
            type = "FOLLOWERINFO";
            break;    
        case Leader.NEWLEADER:
            type = "NEWLEADER";
            break;
        case Leader.PING:
            type = "PING";
            break;
        case Leader.PROPOSAL:
            type = "PROPOSAL";
            TxnHeader hdr = new TxnHeader();
            try {
                txn = SerializeUtils.deserializeTxn(p.getData(), hdr);
                // mess = "transaction: " + txn.toString();
            } catch (IOException e) {
                LOG.warn("Unexpected exception",e);
            }
            break;
        case Leader.REQUEST:
            type = "REQUEST";
            break;
        case Leader.REVALIDATE:
            type = "REVALIDATE";
            ByteArrayInputStream bis = new ByteArrayInputStream(p.getData());
            DataInputStream dis = new DataInputStream(bis);
            try {
                long id = dis.readLong();
                mess = " sessionid = " + id;
            } catch (IOException e) {
                LOG.warn("Unexpected exception", e);
            }

            break;
        case Leader.UPTODATE:
            type = "UPTODATE";
            break;
        default:
            type = "UNKNOWN" + p.getType();
        }
        String entry = null;
        if (type != null) {
            entry = type + " " + Long.toHexString(p.getZxid()) + " " + mess;
        }
        return entry;
    }

    /**
     * This thread will receive packets from the peer and process them and
     * also listen to new connections from new peers.
     */
    @Override
    public void run() {
        try {
            tickOfNextAckDeadline = leader.self.tick + leader.self.initLimit + leader.self.syncLimit;
            LOG.info("tickOfNextAckDeadline : " + tickOfNextAckDeadline);

            ia = BinaryInputArchive.getArchive(new BufferedInputStream(sock.getInputStream()));
            bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput);
            // 等待 Follower 发来数据包
            QuorumPacket qp = new QuorumPacket();
            long a1 = System.currentTimeMillis();
            ia.readRecord(qp, "packet");                                                             // 读取 Follower 发过来的 FOLLOWERINFO 数据包
            LOG.info("System.currentTimeMillis() - a1 : " + (System.currentTimeMillis() - a1));
            LOG.info("qp:" + qp);

            if(qp.getType() != Leader.FOLLOWERINFO && qp.getType() != Leader.OBSERVERINFO){         // 不应该有这种数据的存在
            	LOG.error("First packet " + qp.toString() + " is not FOLLOWERINFO or OBSERVERINFO!");
                return;
            }
            byte learnerInfoData[] = qp.getData();                                                      // 读取参与者发来的数据

            LOG.info("learnerInfoData :" + Arrays.toString(learnerInfoData));                       // 这里的 learnerInfo 就是 Follower/Observer 的信息
            if (learnerInfoData != null) {
            	if (learnerInfoData.length == 8) {
            		ByteBuffer bbsid = ByteBuffer.wrap(learnerInfoData);
            		this.sid = bbsid.getLong();
            	} else {
            		LearnerInfo li = new LearnerInfo();                                                // 反序列化出 LearnerInfo
            		ByteBufferInputStream.byteBuffer2Record(ByteBuffer.wrap(learnerInfoData), li);
                    LOG.info("li :" + li);
            		this.sid = li.getServerid();                                                      // 取出 Follower 的 myid
            		this.version = li.getProtocolVersion();                                          // 通讯的协议
            	}
            } else {
            	this.sid = leader.followerCounter.getAndDecrement();
            }

            LOG.info("Follower sid: " + sid + " : info : " + leader.self.quorumPeers.get(sid));
                        
            if (qp.getType() == Leader.OBSERVERINFO) {
                  learnerType = LearnerType.OBSERVER;
            }            
            
            long lastAcceptedEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());                       // 通过 zxid 来获取 Follower 的 Leader 选举的 epoch

            LOG.info("qp : " + qp + ", lastAcceptedEpoch : " + lastAcceptedEpoch);

            long peerLastZxid;
            StateSummary ss = null;
            long zxid = qp.getZxid();
            long newEpoch = leader.getEpochToPropose(this.getSid(), lastAcceptedEpoch);            // 将 Follower 的 Leader 选举的 epoch  信息加入到 connectingFollowers 里面, 判断 集群中 Leader 选举的 epoch 同步是否 OK

            LOG.info("qp : " + qp + ", newEpoch : " + newEpoch);

            if (this.getVersion() < 0x10000) {                                                      // 这个 if 里面是兼容老的代码
                // we are going to have to extrapolate the epoch information
                long epoch = ZxidUtils.getEpochFromZxid(zxid);
                ss = new StateSummary(epoch, zxid);
                // fake the message
                leader.waitForEpochAck(this.getSid(), ss);
            } else {                                                                                // 发送一个新的 QuorumPacket
                byte ver[] = new byte[4];
                ByteBuffer.wrap(ver).putInt(0x10000);                                               // 构建出 Leader 的信息
                QuorumPacket newEpochPacket = new QuorumPacket(Leader.LEADERINFO, ZxidUtils.makeZxid(newEpoch, 0), ver, null);
                LOG.info("newEpochPacket:" + newEpochPacket);
                oa.writeRecord(newEpochPacket, "packet");                                        // 将 Leader 的信息发送给对应的 Follower / Observer
                bufferedOutput.flush();
                QuorumPacket ackEpochPacket = new QuorumPacket();
                ia.readRecord(ackEpochPacket, "packet");                                          // Leader 读取 Follower 发来的 ACKEPOCH 信息

                LOG.info("ackEpochPacket:" +ackEpochPacket);                                    // 刚刚发送了 leader 的信息, 现在获取一下确认的 ack

                if (ackEpochPacket.getType() != Leader.ACKEPOCH) {
                    LOG.error(ackEpochPacket.toString()
                            + " is not ACKEPOCH");
                    return;
				}
                ByteBuffer bbepoch = ByteBuffer.wrap(ackEpochPacket.getData());
                ss = new StateSummary(bbepoch.getInt(), ackEpochPacket.getZxid());
                LOG.info("ss : " + ss);
                leader.waitForEpochAck(this.getSid(), ss);                                       // 在这边等待所有的 Follower 都回复 ACKEPOCH 值 (这里也是满足过半就可以)

            }
            peerLastZxid = ss.getLastZxid();
            LOG.info("peerLastZxid : " + peerLastZxid);
            
            /* the default to send to the follower */
            int packetToSend = Leader.SNAP;                                                     // 默认的发送一个 SNAP 的数据包
            long zxidToSend = 0;
            long leaderLastZxid = 0;
            /** the packets that the follower needs to get updates from **/
            long updates = peerLastZxid;
            
            /* we are sending the diff check if we have proposals in memory to be able to 
             * send a diff to the 
             */ 
            ReentrantReadWriteLock lock = leader.zk.getZKDatabase().getLogLock();
            ReadLock rl = lock.readLock();
            try {
                rl.lock();        
                final long maxCommittedLog = leader.zk.getZKDatabase().getmaxCommittedLog();         // zookeeper 会缓存 maxCommittedLog -> minCommittedLog 之间的事务
                final long minCommittedLog = leader.zk.getZKDatabase().getminCommittedLog();

                LOG.info("sid:" + sid + ", maxCommittedLog:" + Long.toHexString(maxCommittedLog)
                                        + ", minCommittedLog:" +Long.toHexString(minCommittedLog)
                                        + " peerLastZxid=0x"+Long.toHexString(peerLastZxid)
                );

                LinkedList<Proposal> proposals = leader.zk.getZKDatabase().getCommittedLog();           // 查看是否还有需要的投票
                LOG.info("proposals:"+proposals);
                if (proposals.size() != 0) {                                                            // 处理这些还需要的投票
                    LOG.debug("proposal size is {}", proposals.size());                             // 如果 follower 还没有处理这个事务, 有可能是 down后又恢复了, 则继续处理

                    if ((maxCommittedLog >= peerLastZxid) && (minCommittedLog <= peerLastZxid)) {
                        LOG.info("sid:" + sid + ", maxCommittedLog:" + Long.toHexString(maxCommittedLog)
                                + ", minCommittedLog:" +Long.toHexString(minCommittedLog)
                                + " peerLastZxid=0x"+Long.toHexString(peerLastZxid)
                        );
                        LOG.debug("Sending proposals to follower");

                        // as we look through proposals, this variable keeps track of previous
                        // proposal Id.
                        long prevProposalZxid = minCommittedLog;

                        // Keep track of whether we are about to send the first packet.
                        // Before sending the first packet, we have to tell the learner
                        // whether to expect a trunc or a diff
                        boolean firstPacket=true;

                        // If we are here, we can use committedLog to sync with
                        // follower. Then we only need to decide whether to
                        // send trunc or not
                        packetToSend = Leader.DIFF;
                        zxidToSend = maxCommittedLog;

                        for (Proposal propose: proposals) {
                            // skip the proposals the peer already has                                   // 这个 Propose 已经处理过了, continue
                            if (propose.packet.getZxid() <= peerLastZxid) {
                                prevProposalZxid = propose.packet.getZxid();
                                continue;
                            } else {
                                // If we are sending the first packet, figure out whether to trunc
                                // in case the follower has some proposals that the leader doesn't
                                if (firstPacket) {                                                      // 在发起 Proposal 之前一定要确认 是否 follower 比 Leader 超前处理 Proposal
                                    firstPacket = false;
                                    // Does the peer have some proposals that the leader hasn't seen yet
                                    if (prevProposalZxid < peerLastZxid) {                             // follower 的处理事务处理比 leader 多, 则发送 TRUC 进行 Proposal 数据同步
                                        // send a trunc message before sending the diff
                                        packetToSend = Leader.TRUNC;                                        
                                        zxidToSend = prevProposalZxid;
                                        updates = zxidToSend;
                                    }
                                }
                                queuePacket(propose.packet);                                          // 将 事务发送到 发送队列里面
                                QuorumPacket qcommit = new QuorumPacket(Leader.COMMIT, propose.packet.getZxid(),
                                        null, null);
                                queuePacket(qcommit);
                            }
                        }
                    } else if (peerLastZxid > maxCommittedLog) {                                      // follower 的处理事务处理比 leader 多, 则发送 TRUC 进行 Proposal 数据同步
                        LOG.debug("Sending TRUNC to follower zxidToSend=0x{} updates=0x{}",
                                Long.toHexString(maxCommittedLog),
                                Long.toHexString(updates));

                        LOG.info("sid:" + sid + ", maxCommittedLog:" + Long.toHexString(maxCommittedLog)
                                + ", minCommittedLog:" +Long.toHexString(minCommittedLog)
                                + " peerLastZxid=0x"+Long.toHexString(peerLastZxid)
                                + ", updates : " + Long.toHexString(updates)
                        );

                        packetToSend = Leader.TRUNC;
                        zxidToSend = maxCommittedLog;
                        updates = zxidToSend;
                    } else {
                        LOG.warn("Unhandled proposal scenario");
                    }                                                                                  // 若 Follower 与 Leader 的 lastZxid 相同, 则 发送 DIFF
                } else if (peerLastZxid == leader.zk.getZKDatabase().getDataTreeLastProcessedZxid()) {
                    // The leader may recently take a snapshot, so the committedLog
                    // is empty. We don't need to send snapshot if the follow
                    // is already sync with in-memory db.
                    LOG.info("committedLog is empty but leader and follower "
                                    + "are in sync, zxid=0x{}",
                            Long.toHexString(peerLastZxid));

                    LOG.info("sid:" + sid + ", maxCommittedLog:" + Long.toHexString(maxCommittedLog)
                            + ", minCommittedLog:" +Long.toHexString(minCommittedLog)
                            + " peerLastZxid=0x"+Long.toHexString(peerLastZxid)
                    );

                    packetToSend = Leader.DIFF;
                    zxidToSend = peerLastZxid;
                } else {
                    // just let the state transfer happen
                    LOG.debug("proposals is empty");
                }               

                LOG.info("Sending " + Leader.getPacketType(packetToSend));
                leaderLastZxid = leader.startForwarding(this, updates);
                LOG.info("leaderLastZxid : " + leaderLastZxid);

            } finally {
                rl.unlock();
            }
                                                                                                 // 发送 NEWLEADER 数据包
             QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER, ZxidUtils.makeZxid(newEpoch, 0), null, null);
             LOG.info("newLeaderQP:" + newLeaderQP);
             if (getVersion() < 0x10000) {
                oa.writeRecord(newLeaderQP, "packet");
            } else {
                queuedPackets.add(newLeaderQP);
            }
            bufferedOutput.flush();
            //Need to set the zxidToSend to the latest zxid
            LOG.info("packetToSend : " + packetToSend);
            if (packetToSend == Leader.SNAP) {
                zxidToSend = leader.zk.getZKDatabase().getDataTreeLastProcessedZxid();
            }
            QuorumPacket quorumPackett2 = new QuorumPacket(packetToSend, zxidToSend, null, null);
            LOG.info("quorumPackett2 : " + quorumPackett2);
            oa.writeRecord(quorumPackett2, "packet");
            bufferedOutput.flush();
            
            /* if we are not truncating or sending a diff just send a snapshot */
            if (packetToSend == Leader.SNAP) {
                LOG.info("Sending snapshot last zxid of peer is 0x"
                        + Long.toHexString(peerLastZxid) + " " 
                        + " zxid of leader is 0x"
                        + Long.toHexString(leaderLastZxid)
                        + "sent zxid of db as 0x" 
                        + Long.toHexString(zxidToSend));
                // Dump data to peer
                LOG.info("将 Leader 的数据 序列化到数据流 oa 里面");
                leader.zk.getZKDatabase().serializeSnapshot(oa);
                LOG.info("将 Leader 的数据 序列化到数据流 oa 里面 OK ");
                oa.writeString("BenWasHere", "signature");
            }
            bufferedOutput.flush();
            
            // Start sending packets                                                                    // 启动一个线程进行数据包的发送
            new Thread() {
                public void run() {
                    Thread.currentThread().setName("Sender-" + sock.getRemoteSocketAddress());
                    try {
                        sendPackets();
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected interruption",e);
                    }
                }
            }.start();
            
            /*
             * Have to wait for the first ACK, wait until 
             * the leader is ready, and only then we can
             * start processing messages.
             */
            qp = new QuorumPacket();
            ia.readRecord(qp, "packet");                                                             // 读取 Follower 发来的数据包
            LOG.info("qp:" + qp);


            if(qp.getType() != Leader.ACK){
                LOG.error("Next packet was supposed to be an ACK");
                return;
            }
            LOG.info("Received NEWLEADER-ACK message from " + getSid());
            LOG.info("getSid():" + getSid() + ", qp.getZxid():" + qp.getZxid() + ", getLearnerType():" + getLearnerType());
            leader.waitForNewLeaderAck(getSid(), qp.getZxid(), getLearnerType());

            syncLimitCheck.start();
            
            // now that the ack has been processed expect the syncLimit
            sock.setSoTimeout(leader.self.tickTime * leader.self.syncLimit);

            /*
             * Wait until leader starts up
             */
            synchronized(leader.zk){                                                               // 等待 leader 恢复执行
                while(!leader.zk.isRunning() && !this.isInterrupted()){
                    leader.zk.wait(20);
                }
            }
            LOG.info("leader.zk.isRunning() :" + leader.zk.isRunning());

            // Mutation(突变) packets will be queued during the serialize,
            // so we need to mark when the peer can actually start
            // using the data
            //
            queuedPackets.add(new QuorumPacket(Leader.UPTODATE, -1, null, null));               // leader 启动, 发送一个 UPTODATE 数据包

            while (true) {
                qp = new QuorumPacket();
                ia.readRecord(qp, "packet");                                                        // 这里其实就是不断的从数据流(来源于 Follower 的) 读取数据
                LOG.info("qp:" + qp);

                long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
                if (qp.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'i', qp);
                }
                tickOfNextAckDeadline = leader.self.tick + leader.self.syncLimit;
                LOG.info("tickOfNextAckDeadline :" + tickOfNextAckDeadline);

                ByteBuffer bb;
                long sessionId;
                int cxid;
                int type;

                LOG.info("qp.getType() : " + qp);
                switch (qp.getType()) {
                case Leader.ACK:                                                                              // 处理 Follower 回复给 Leader 的ACK 包看看之前的投票是否结束
                    if (this.learnerType == LearnerType.OBSERVER) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Received ACK from Observer  " + this.sid);
                        }
                    }
                    LOG.info("syncLimitCheck.updateAck(qp.getZxid()):"  + qp.getZxid());
                    syncLimitCheck.updateAck(qp.getZxid());
                    LOG.info("this.sid:" + this.sid + ", qp.getZxid():" + qp.getZxid() + ", sock.getLocalSocketAddress():" + sock.getLocalSocketAddress());
                    leader.processAck(this.sid, qp.getZxid(), sock.getLocalSocketAddress());                // ack 包处理成功, 如果 follower 数据同步成功, 则将它添加到 NEWLEADER 这个投票的结果中
                    break;
                case Leader.PING:                                                                            // ping 数据包, 更新 session 的超时时间
                    // Process the touches
                    ByteArrayInputStream bis = new ByteArrayInputStream(qp.getData());
                    DataInputStream dis = new DataInputStream(bis);
                    while (dis.available() > 0) {
                        long sess = dis.readLong();
                        int to = dis.readInt();
                        LOG.info("leader.zk.touch: sess" + sess + ", to:"+to);
                        leader.zk.touch(sess, to);
                    }
                    break;
                case Leader.REVALIDATE:                                                                     // 检查 session 是否还存活
                    bis = new ByteArrayInputStream(qp.getData());
                    dis = new DataInputStream(bis);
                    long id = dis.readLong();
                    int to = dis.readInt();
                    LOG.info("id:"+id + ", to:" + to);
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(bos);
                    dos.writeLong(id);
                    boolean valid = leader.zk.touch(id, to);
                    LOG.info("id:" + id + ", to:" + to + ", valid:" + valid);
                    if (valid) {
                        try {
                            //set the session owner
                            // as the follower that
                            // owns the session
                            leader.zk.setOwner(id, this);
                        } catch (SessionExpiredException e) {
                            LOG.error("Somehow session " + Long.toHexString(id) + " expired right after being renewed! (impossible)", e);
                        }
                    }
                    if (LOG.isTraceEnabled()) {
                        ZooTrace.logTraceMessage(LOG,
                                                 ZooTrace.SESSION_TRACE_MASK,
                                                 "Session 0x" + Long.toHexString(id)
                                                 + " is valid: "+ valid);
                    }
                    dos.writeBoolean(valid);
                    qp.setData(bos.toByteArray());
                    queuedPackets.add(qp);                                                               // 将数据包返回给对应的 follower
                    break;
                case Leader.REQUEST:                                                                      // REQUEST 数据包, follower 会将事务请求转发给 leader 进行处理
                    bb = ByteBuffer.wrap(qp.getData());
                    sessionId = bb.getLong();
                    cxid = bb.getInt();
                    type = bb.getInt();
                    bb = bb.slice();                                                                       // 读取事务信息
                    Request si;
                    LOG.info(" sessionId:" + sessionId + ", cxid:" + cxid + ", type:" + type);
                    if(type == OpCode.sync){
                        si = new LearnerSyncRequest(this, sessionId, cxid, type, bb, qp.getAuthinfo());
                    } else {
                        si = new Request(null, sessionId, cxid, type, bb, qp.getAuthinfo());
                    }
                    si.setOwner(this);
                    LOG.info("si:" + si);
                    leader.zk.submitRequest(si);                                                         // 将事务请求的信息交由 Leader 的 RequestProcessor 处理
                    break;
                default:
                }
            }
        } catch (IOException e) {
            if (sock != null && !sock.isClosed()) {
                LOG.error("Unexpected exception causing shutdown while sock "
                        + "still open", e);
            	//close the socket to make sure the 
            	//other side can see it being close
            	try {
            		sock.close();
            	} catch(IOException ie) {
            		// do nothing
            	}
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected exception causing shutdown", e);
        } finally {
            LOG.warn("******* GOODBYE " 
                    + (sock != null ? sock.getRemoteSocketAddress() : "<null>")
                    + " ********");
            shutdown();
        }
    }

    // 进行 LearnerHandler 的关闭操作, 其实就是通过在 queuedPackets 里面加入一个 proposalOfDeath
    public void shutdown() {
        // Send the packet of death
        try {
            queuedPackets.put(proposalOfDeath);
        } catch (InterruptedException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }
        try {
            if (sock != null && !sock.isClosed()) {
                sock.close();
            }
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during socket close", e);
        }
        this.interrupt();
        leader.removeLearnerHandler(this);
    }

    public long tickOfNextAckDeadline() {
        return tickOfNextAckDeadline;
    }

    /**
     * ping calls from the leader to the peers
     */
    public void ping() {
        long id;
        if (syncLimitCheck.check(System.nanoTime())) {
            synchronized(leader) {
                id = leader.lastProposed;
            }
            QuorumPacket ping = new QuorumPacket(Leader.PING, id, null, null);
            queuePacket(ping);
        } else {
            LOG.warn("Closing connection to peer due to transaction timeout.");
            shutdown();
        }
    }

    void queuePacket(QuorumPacket p) {
        queuedPackets.add(p);
    }

    public boolean synced() {
        return isAlive()
        && leader.self.tick <= tickOfNextAckDeadline;
    }
}

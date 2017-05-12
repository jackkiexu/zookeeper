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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 参考资料
 * http://blog.csdn.net/vinowan/article/details/22197461
 *
 * This class has the control logic for the Leader.
 */
public class Leader {
    private static final Logger LOG = LoggerFactory.getLogger(Leader.class);
    
    static final private boolean nodelay = System.getProperty("leader.nodelay", "true").equals("true");
    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }

    static public class Proposal {
        public QuorumPacket packet;             // 这个 packet 用于发送出去的
        // 每个 Proposal 对应一个 ackSet, 只有 ackSet 里面的ack数量在集群中过半才能真正的进行提交
        public HashSet<Long> ackSet = new HashSet<Long>();
        public Request request;                 // 这里的 request 才是发送的正真 事务消息

        @Override
        public String toString() {
            return "Proposal{" +
                    "packet=" + packet +
                    ", ackSet=" + ackSet +
                    ", request=" + request +
                    '}';
        }
    }

    final LeaderZooKeeperServer zk;

    final QuorumPeer self;

    private boolean quorumFormed = false;
    
    // the follower acceptor thread
    LearnerCnxAcceptor cnxAcceptor;
    
    // list of all the followers
    private final HashSet<LearnerHandler> learners =
        new HashSet<LearnerHandler>();

    /**
     * Returns a copy of the current learner snapshot
     */
    public List<LearnerHandler> getLearners() {
        synchronized (learners) {
            return new ArrayList<LearnerHandler>(learners);
        }
    }

    // list of followers that are ready to follow (i.e synced with the leader)
    private final HashSet<LearnerHandler> forwardingFollowers =
        new HashSet<LearnerHandler>();
    
    /**
     * Returns a copy of the current forwarding follower snapshot
     */
    public List<LearnerHandler> getForwardingFollowers() {
        synchronized (forwardingFollowers) {
            return new ArrayList<LearnerHandler>(forwardingFollowers);
        }
    }

    private void addForwardingFollower(LearnerHandler lh) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.add(lh);
        }
    }

    private final HashSet<LearnerHandler> observingLearners =
        new HashSet<LearnerHandler>();
        
    /**
     * Returns a copy of the current observer snapshot
     */
    public List<LearnerHandler> getObservingLearners() {
        synchronized (observingLearners) {
            return new ArrayList<LearnerHandler>(observingLearners);
        }
    }

    private void addObserverLearnerHandler(LearnerHandler lh) {
        synchronized (observingLearners) {
            observingLearners.add(lh);
        }
    }

    // Pending sync requests. Must access under 'this' lock.
    private final HashMap<Long,List<LearnerSyncRequest>> pendingSyncs =
        new HashMap<Long,List<LearnerSyncRequest>>();
    
    synchronized public int getNumPendingSyncs() {
        return pendingSyncs.size();
    }

    //Follower counter
    final AtomicLong followerCounter = new AtomicLong(-1);

    /**
     * Adds peer to the leader.
     * 
     * @param learner
     *                instance of learner handle
     */
    void addLearnerHandler(LearnerHandler learner) {
        synchronized (learners) {
            learners.add(learner);
        }
    }

    /**
     * Remove the learner from the learner list
     * 
     * @param peer
     */
    void removeLearnerHandler(LearnerHandler peer) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.remove(peer);            
        }        
        synchronized (learners) {
            learners.remove(peer);
        }
        synchronized (observingLearners) {
            observingLearners.remove(peer);
        }
    }

    boolean isLearnerSynced(LearnerHandler peer){
        synchronized (forwardingFollowers) {
            return forwardingFollowers.contains(peer);
        }        
    }
    
    ServerSocket ss;

    // 初始化 Leader, 并监听对应额端口, 这个端口是用于连接 Leader 与 Follower 之间的消息通信
    Leader(QuorumPeer self,LeaderZooKeeperServer zk) throws IOException {
        this.self = self;
        try {
            if (self.getQuorumListenOnAllIPs()) {
                ss = new ServerSocket(self.getQuorumAddress().getPort());
            } else {
                ss = new ServerSocket();
            }
            ss.setReuseAddress(true);
            if (!self.getQuorumListenOnAllIPs()) {
                ss.bind(self.getQuorumAddress());       // 绑定端口
            }
            LOG.info("Socket:" + ss);
        } catch (BindException e) {
            if (self.getQuorumListenOnAllIPs()) {
                LOG.error("Couldn't bind to port " + self.getQuorumAddress().getPort(), e);
            } else {
                LOG.error("Couldn't bind to " + self.getQuorumAddress(), e);
            }
            throw e;
        }
        this.zk=zk;
    }

    /**
     * This message is for follower to expect diff
     */
    final static int DIFF = 13;
    
    /**
     * This is for follower to truncate its logs 
     */
    final static int TRUNC = 14;
    
    /**
     * This is for follower to download the snapshots
     */
    final static int SNAP = 15;
    
    /**
     * This tells the leader that the connecting peer is actually an observer
     */
    final static int OBSERVERINFO = 16;
    
    /**
     * This message type is sent by the leader to indicate it's zxid and if
     * needed, its database.
     */
    final static int NEWLEADER = 10;

    /**
     * This message type is sent by a follower to pass the last zxid. This is here
     * for backward compatibility purposes.
     */
    final static int FOLLOWERINFO = 11;

    /**
     * This message type is sent by the leader to indicate that the follower is
     * now uptodate andt can start responding to clients.
     */
    final static int UPTODATE = 12;

    /**
     * This message is the first that a follower receives from the leader.
     * It has the protocol version and the epoch of the leader.
     */
    public static final int LEADERINFO = 17;

    /**
     * This message is used by the follow to ack a proposed epoch.
     */
    public static final int ACKEPOCH = 18;
    
    /**
     * This message type is sent to a leader to request and mutation operation.
     * The payload will consist of a request header followed by a request.
     */
    final static int REQUEST = 1;

    /**
     * This message type is sent by a leader to propose a mutation.
     */
    public final static int PROPOSAL = 2;

    /**
     * This message type is sent by a follower after it has synced a proposal.
     */
    // 在 Follower 已经有了一个 同步数据的 Proposal 后, 然后就会发送 ACK 进行响应
    final static int ACK = 3;

    /**
     * This message type is sent by a leader to commit a proposal and cause
     * followers to start serving the corresponding data.
     */
    final static int COMMIT = 4;

    /**
     * This message type is enchanged between follower and leader (initiated by
     * follower) to determine liveliness.
     */
    final static int PING = 5;

    /**
     * This message type is to validate a session that should be active.
     */
    final static int REVALIDATE = 6;

    /**
     * This message is a reply to a synchronize command flushing the pipe
     * between the leader and the follower.
     */
    final static int SYNC = 7;
        
    /**
     * This message type informs observers of a committed proposal.
     */
    final static int INFORM = 8;

    ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();

    ConcurrentLinkedQueue<Proposal> toBeApplied = new ConcurrentLinkedQueue<Proposal>();

    Proposal newLeaderProposal = new Proposal();

    // 一个 Leader 用于监听端口, 其他 Learner 连接上来进行消息发送
    class LearnerCnxAcceptor extends Thread{
        private volatile boolean stop = false;
        
        @Override
        public void run() {
            try {
                while (!stop) {
                    try{
                        Socket s = ss.accept();                                     // Leader 在指定的端口进行监听
                        // start with the initLimit, once the ack is processed
                        // in LearnerHandler switch to the syncLimit
                        s.setSoTimeout(self.tickTime * self.initLimit);        // 这里的 soTimeout 影响的是 InputStream.read 方法, 读取数据时, 超过这个时间, 就会出现异常
                        s.setTcpNoDelay(nodelay);                                 // 设置不适用 合并小的数据包, 重而减少带宽的算法
                        LOG.info("LearnerCnxAcceptor has accept socket :" + s);
                        LearnerHandler fh = new LearnerHandler(s, Leader.this);   // 每个连接上来的 Follower/Observer 都需要一个 LearnerHandler 与进行处理
                        fh.start();
                    } catch (SocketException e) {
                        if (stop) {
                            LOG.info("exception while shutting down acceptor: "
                                    + e);

                            // When Leader.shutdown() calls ss.close(),
                            // the call to accept throws an exception.
                            // We catch and set stop to true.
                            stop = true;
                        } else {
                            throw e;
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception while accepting follower", e);
            }
        }
        
        public void halt() {
            stop = true;
        }
    }

    StateSummary leaderStateSummary;
    
    long epoch = -1;
    boolean waitingForNewEpoch = true;
    volatile boolean readyToStart = false;
    
    /**
     * This method is main function that is called to lead
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    void lead() throws IOException, InterruptedException {
        self.end_fle = System.currentTimeMillis();
        LOG.info("LEADING - LEADER ELECTION TOOK - " + (self.end_fle - self.start_fle));
        self.start_fle = 0;
        self.end_fle = 0;

        zk.registerJMX(new LeaderBean(this, zk), self.jmxLocalPeerBean);

        try {
            self.tick = 0;
            zk.loadData();                                                                                  // 从 snapshot, txn log 里面进行数据的恢复
            
            leaderStateSummary = new StateSummary(self.getCurrentEpoch(), zk.getLastProcessedZxid());   // 生成 Leader 的状态信息
            LOG.info("leaderStateSummary:" + leaderStateSummary);
            // Start thread that waits for connection requests from 
            // new followers.
            cnxAcceptor = new LearnerCnxAcceptor();                                                       // LearnerCnxAcceptor 它会监听在对应端口, 一有 follower 连接上, 就开启一个 LearnerHandler 来处理对应的事件
            LOG.info("cnxAcceptor start");
            cnxAcceptor.start();
            
            readyToStart = true;
            LOG.info("self.getId() :" + self.getId() + ",  self.getAcceptedEpoch():" +  self.getAcceptedEpoch()); // 一开始这个 getAcceptedEpoch 是直接从文件中恢复过来的, 指的是处理过的 Propose
            long epoch = getEpochToPropose(self.getId(), self.getAcceptedEpoch());                        // 等待足够多de Follower进来, 代表自己确实是 leader, 此处 lead 线程可能在 while 循环处等待
            LOG.info("epoch:"+epoch);
            zk.setZxid(ZxidUtils.makeZxid(epoch, 0));
            
            synchronized(this){
                lastProposed = zk.getZxid();
                LOG.info("lastProposed:"+lastProposed);
            }
            
            newLeaderProposal.packet = new QuorumPacket(NEWLEADER, zk.getZxid(),                    // 构建一个 NEWLEADER 的数据包
                    null, null);


            if ((newLeaderProposal.packet.getZxid() & 0xffffffffL) != 0) {
                LOG.info("NEWLEADER proposal has Zxid of "
                        + Long.toHexString(newLeaderProposal.packet.getZxid()));
            }
            
            waitForEpochAck(self.getId(), leaderStateSummary);                                        // 等待投票满足过半ed原则
            self.setCurrentEpoch(epoch);

            // We have to get at least a majority of servers in sync with
            // us. We do this by waiting for the NEWLEADER packet to get
            // acknowledged
            try {
                waitForNewLeaderAck(self.getId(), zk.getZxid(), LearnerType.PARTICIPANT);
            } catch (InterruptedException e) {
                shutdown("Waiting for a quorum of followers, only synced with sids: [ "
                        + getSidSetString(newLeaderProposal.ackSet) + " ]");
                HashSet<Long> followerSet = new HashSet<Long>();
                for (LearnerHandler f : learners)
                    followerSet.add(f.getSid());
                    
                if (self.getQuorumVerifier().containsQuorum(followerSet)) {
                    LOG.warn("Enough followers present. "
                            + "Perhaps the initTicks need to be increased.");
                }
                Thread.sleep(self.tickTime);
                self.tick++;
                return;
            }
            // 在所有 Follower 与 Leader 进行 选举 epoch, 数据内容同步好之后, Leader 开启 ZooKeeperServer 服务
            startZkServer();
            
            /**
             * WARNING: do not use this for anything other than QA testing
             * on a real cluster. Specifically to enable verification that quorum
             * can handle the lower 32bit roll-over issue identified in
             * ZOOKEEPER-1277. Without this option it would take a very long
             * time (on order of a month say) to see the 4 billion writes
             * necessary to cause the roll-over to occur.
             * 
             * This field allows you to override the zxid of the server. Typically
             * you'll want to set it to something like 0xfffffff0 and then
             * start the quorum, run some operations and see the re-election.
             */
            String initialZxid = System.getProperty("zookeeper.testingonly.initialZxid");
            if (initialZxid != null) {
                long zxid = Long.parseLong(initialZxid);
                zk.setZxid((zk.getZxid() & 0xffffffff00000000L) | zxid);
            }
            
            if (!System.getProperty("zookeeper.leaderServes", "yes").equals("no")) {
                self.cnxnFactory.setZooKeeperServer(zk);
            }
            // Everything is a go, simply start counting the ticks
            // WARNING: I couldn't find any wait statement on a synchronized
            // block that would be notified by this notifyAll() call, so
            // I commented it out
            //synchronized (this) {
            //    notifyAll();
            //}
            // We ping twice a tick, so we only update the tick every other
            // iteration
            boolean tickSkip = true;
    
            while (true) {
                Thread.sleep(self.tickTime / 2);                                                 //
                if (!tickSkip) {
                    self.tick++;
                }
                HashSet<Long> syncedSet = new HashSet<Long>();

                // lock on the followers when we use it.
                syncedSet.add(self.getId());

                for (LearnerHandler f : getLearners()) {                                            // 检查每个 follower 是否存活
                    // Synced set is used to check we have a supporting quorum, so only
                    // PARTICIPANT, not OBSERVER, learners should be used
                    if (f.synced() && f.getLearnerType() == LearnerType.PARTICIPANT) {
                        syncedSet.add(f.getSid());
                    }
                    f.ping();                                                                       // 这里的 ping 其实就是 Follower 将 session 发送给 Leader 来进行校验超时
                }

              if (!tickSkip && !self.getQuorumVerifier().containsQuorum(syncedSet)) {               // 如果有 follower 挂掉导致投票不通过, 则退出 lead 流程, 重新选举
                //if (!tickSkip && syncedCount < self.quorumPeers.size() / 2) {
                    // Lost quorum, shutdown
                    shutdown("Not sufficient followers synced, only synced with sids: [ "
                            + getSidSetString(syncedSet) + " ]");
                    // make sure the order is the same!
                    // the leader goes to looking
                    return;
              } 
              tickSkip = !tickSkip;
            }
        } finally {
            zk.unregisterJMX(this);
        }
    }

    boolean isShutdown;

    /**
     * Close down all the LearnerHandlers
     */
    void shutdown(String reason) {
        LOG.info("Shutting down");

        if (isShutdown) {
            return;
        }
        
        LOG.info("Shutdown called",
                new Exception("shutdown Leader! reason: " + reason));

        if (cnxAcceptor != null) {
            cnxAcceptor.halt();
        }
        
        // NIO should not accept conenctions
        self.cnxnFactory.setZooKeeperServer(null);
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during close",e);
        }
        // clear all the connections
        self.cnxnFactory.closeAll();
        // shutdown the previous zk
        if (zk != null) {
            zk.shutdown();
        }
        synchronized (learners) {
            for (Iterator<LearnerHandler> it = learners.iterator(); it
                    .hasNext();) {
                LearnerHandler f = it.next();
                it.remove();
                f.shutdown();
            }
        }
        isShutdown = true;
    }

    /**
     * 参考资料
     * http://blog.csdn.net/vinowan/article/details/22196707
     *
     * Keep a count of acks that are received by the leader for a particular
     * proposal
     * 
     * @param zxid the zxid of the proposal sent out
     */
    synchronized public void processAck(long sid, long zxid, SocketAddress followerAddr) {
        LOG.info("sid:" + sid + ", zxid:" + zxid + ", followerAddr:" + followerAddr);
        if (LOG.isTraceEnabled()) {
            LOG.trace("Ack zxid: 0x{}", Long.toHexString(zxid));
            for (Proposal p : outstandingProposals.values()) {
                long packetZxid = p.packet.getZxid();
                LOG.trace("outstanding proposal: 0x{}",
                        Long.toHexString(packetZxid));
            }
            LOG.trace("outstanding proposals all");
        }

        LOG.info("(zxid & 0xffffffffL) == 0 :" + ((zxid & 0xffffffffL) == 0));
        if ((zxid & 0xffffffffL) == 0) {                                                // zxid 全是 0
            /*
             * We no longer process NEWLEADER ack by this method. However,
             * the learner sends ack back to the leader after it gets UPTODATE
             * so we just ignore the message.
             */
            return;
        }

        LOG.info("outstandingProposals :" + outstandingProposals);
        if (outstandingProposals.size() == 0) {                                     // 没有要回应 ack 的 Proposal 存在
            if (LOG.isDebugEnabled()) {
                LOG.debug("outstanding is 0");
            }
            return;
        }
        LOG.info("lastCommitted :" + lastCommitted + ", zxid:" + zxid);
        if (lastCommitted >= zxid) {                                                // Leader 端处理的 lastCommited >= zxid, 说明 zxid 对应的 proposal 已经处理过了
            if (LOG.isDebugEnabled()) {
                LOG.debug("proposal has already been committed, pzxid: 0x{} zxid: 0x{}", Long.toHexString(lastCommitted), Long.toHexString(zxid));
            }
            // The proposal has already been committed
            return;
        }
        Proposal p = outstandingProposals.get(zxid);                               // 从投票箱 outstandingProposals 获取 zxid 对应的 Proposal
        LOG.info("p:" + p);
        if (p == null) {
            LOG.warn("Trying to commit future proposal: zxid 0x{} from {}", Long.toHexString(zxid), followerAddr);
            return;
        }
        LOG.info("p:" + p + ", sid:" + sid);

        p.ackSet.add(sid);                                                                  // 将 follower 的 myid 加入结果列表
        if (LOG.isDebugEnabled()) {
            LOG.info("Count for zxid: 0x{} is {}", Long.toHexString(zxid), p.ackSet.size());
        }

        LOG.info("self.getQuorumVerifier().containsQuorum(p.ackSet):" + self.getQuorumVerifier().containsQuorum(p.ackSet));
        if (self.getQuorumVerifier().containsQuorum(p.ackSet)){                            // 判断是否票数够了, 则启动  leader 的 CommitProcessor 来进行处理

            LOG.info("zxid:" + zxid + ", lastCommitted:" + lastCommitted);
            if (zxid != lastCommitted+1) {
                LOG.warn("Commiting zxid 0x{} from {} not first!", Long.toHexString(zxid), followerAddr);
                LOG.warn("First is 0x{}", Long.toHexString(lastCommitted + 1));
            }

            LOG.info("outstandingProposals:" + outstandingProposals);
            outstandingProposals.remove(zxid);                                           // 从 outstandingProposals 里面删除那个可以提交的 Proposal
            if (p.request != null) {
                toBeApplied.add(p);                                                       // 加入到 toBeApplied 队列里面, 这里的 toBeApplied 是 ToBeAppliedRequestProcessor, Leader 共用的队列, 在经过 CommitProcessor 处理过后, 就到 ToBeAppliedRequestProcessor 里面进行处理
                LOG.info("toBeApplied:" + toBeApplied);
            }

            if (p.request == null) {
                LOG.warn("Going to commmit null request for proposal: {}", p);
            }
            commit(zxid);                                                                   // 向 集群中的 Followers 发送 commit 消息, 来通知大家, zxid 对应的 Proposal 可以 commit 了
            inform(p);                                                                      // 向 集群中的 Observers 发送 commit 消息, 来通知大家, zxid 对应的 Proposal 可以 commit 了
            zk.commitProcessor.commit(p.request);                                       // 自己进行 proposal 的提交 (直接调用 commitProcessor 进行提交 )

            LOG.info("pendingSyncs :" + pendingSyncs);
            if(pendingSyncs.containsKey(zxid)){
                for(LearnerSyncRequest r: pendingSyncs.remove(zxid)) {
                    sendSync(r);
                }
            }
        }
    }

    /**
     * 维护 Leader 类的 toBeApplied 队列, 这个队列中保存着已经完成 投票的 Request 事件
     */
    static class ToBeAppliedRequestProcessor implements RequestProcessor {
        private RequestProcessor next;

        private ConcurrentLinkedQueue<Proposal> toBeApplied;

        /**
         * This request processor simply maintains the toBeApplied list. For
         * this to work next must be a FinalRequestProcessor and
         * FinalRequestProcessor.processRequest MUST process the request
         * synchronously!
         * 
         * @param next
         *                a reference to the FinalRequestProcessor
         */
        ToBeAppliedRequestProcessor(RequestProcessor next,
                ConcurrentLinkedQueue<Proposal> toBeApplied) {
            if (!(next instanceof FinalRequestProcessor)) {
                throw new RuntimeException(ToBeAppliedRequestProcessor.class
                        .getName()
                        + " must be connected to "
                        + FinalRequestProcessor.class.getName()
                        + " not "
                        + next.getClass().getName());
            }
            this.toBeApplied = toBeApplied;
            this.next = next;
        }

        /*
         * (non-Javadoc)
         * 参考
         * http://shift-alt-ctrl.iteye.com/blog/1849545
         * @see org.apache.zookeeper.server.RequestProcessor#processRequest(org.apache.zookeeper.server.Request)
         */
        public void processRequest(Request request) throws RequestProcessorException {
            LOG.info("request:"+ request);
            // request.addRQRec(">tobe");
            next.processRequest(request);                   // 交由 FinalRequestProcessor commit 到 ZKDatabase 里面s
            Proposal p = toBeApplied.peek();               // 进行 request 从 toBeApplied 删除
            if (p != null && p.request != null
                    && p.request.zxid == request.zxid) {
                toBeApplied.remove();
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.zookeeper.server.RequestProcessor#shutdown()
         */
        public void shutdown() {
            LOG.info("Shutting down");
            next.shutdown();
        }
    }

    /**
     * send a packet to all the followers ready to follow
     * 
     * @param qp
     *                the packet to be sent
     */
    // 向 Follower 发送 Proposal 的消息
    void sendPacket(QuorumPacket qp) {
        synchronized (forwardingFollowers) {
            for (LearnerHandler f : forwardingFollowers) {                
                f.queuePacket(qp);
            }
        }
    }
    
    /**
     * send a packet to all observers     
     */
    void sendObserverPacket(QuorumPacket qp) {        
        for (LearnerHandler f : getObservingLearners()) {
            f.queuePacket(qp);
        }
    }

    long lastCommitted = -1;

    /**
     * Create a commit packet and send it to all the members of the quorum
     * 
     * @param zxid
     */
    // Leader 向集群中的各个节点发送 Commit Proposal 通知
    public void commit(long zxid) {
        synchronized(this){
            lastCommitted = zxid;
        }
        QuorumPacket qp = new QuorumPacket(Leader.COMMIT, zxid, null, null);
        LOG.info("qp:" + qp);
        sendPacket(qp);
    }
    
    /**
     * Create an inform packet and send it to all observers.
     * @param proposal
     */
    public void inform(Proposal proposal) {   
        QuorumPacket qp = new QuorumPacket(Leader.INFORM, proposal.request.zxid, proposal.packet.getData(), null);
        LOG.info("qp:" + qp);
        sendObserverPacket(qp);
    }

    long lastProposed;

    
    /**
     * Returns the current epoch of the leader.
     * 
     * @return
     */
    public long getEpoch(){
        return ZxidUtils.getEpochFromZxid(lastProposed);
    }
    
    @SuppressWarnings("serial")
    public static class XidRolloverException extends Exception {
        public XidRolloverException(String message) {
            super(message);
        }
    }

    /**
     * create a proposal and send it out to all the members
     * 创建 一个  Proposal 并且 发送给集群中的 Followers
     * @param request
     * @return the proposal that is queued to send to all the members
     */
    public Proposal propose(Request request) throws XidRolloverException {
        /**
         * Address the rollover issue. All lower 32bits set indicate a new leader
         * election. Force a re-election instead. See ZOOKEEPER-1277
         */
        if ((request.zxid & 0xffffffffL) == 0xffffffffL) {
            String msg = "zxid lower 32 bits have rolled over, forcing re-election, and therefore new epoch start";
            shutdown(msg);
            throw new XidRolloverException(msg);
        }
                                                                                // 将发送的 Proposal 序列化成  byte 数组
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        try {
            request.hdr.serialize(boa, "hdr");
            if (request.txn != null) {
                request.txn.serialize(boa, "txn");
            }
            baos.close();
        } catch (IOException e) {
            LOG.warn("This really should be impossible", e);
        }                                                                       // 组装 Proposal
        QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, request.zxid, baos.toByteArray(), null);
        
        Proposal p = new Proposal();
        p.packet = pp;
        p.request = request;
        synchronized (this) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Proposing:: " + request);
            }

            lastProposed = p.packet.getZxid();
            outstandingProposals.put(lastProposed, p);                   // 将 没有经过 过半 ACK 确认的 Proposal 暂时放在 outstandingProposals 里面; outstandingProposals 相当于投票箱, Proposal 相当于议题,  Proposal.ackSet 相当于 投赞成票的 myid 集合(只要集群中过半 myid 在 Proposal.ackSet中, 则这个议题就会通过)
            sendPacket(pp);                                                   // 将 Proposal 发送给各台 Follower
        }
        return p;
    }
            
    /**
     * Process sync requests
     * 
     * @param r the request
     */
    
    synchronized public void processSync(LearnerSyncRequest r){
        if(outstandingProposals.isEmpty()){
            sendSync(r);
        } else {
            List<LearnerSyncRequest> l = pendingSyncs.get(lastProposed);
            if (l == null) {
                l = new ArrayList<LearnerSyncRequest>();
            }
            l.add(r);
            pendingSyncs.put(lastProposed, l);
        }
    }
        
    /**
     * Sends a sync message to the appropriate server
     * 
     * @param r
     */
            
    public void sendSync(LearnerSyncRequest r){
        QuorumPacket qp = new QuorumPacket(Leader.SYNC, 0, null, null);
        LOG.info("qp:" + qp);
        r.fh.queuePacket(qp);
    }
                
    /**
     * lets the leader know that a follower is capable of following and is done
     * syncing
     *
     * zookeeper 的典型问题
     * http://blog.csdn.net/fei33423/article/details/53749138
     * http://www.jianshu.com/p/4cc1040b6a14
     * http://blog.csdn.net/a040600145/article/details/53842280?utm_source=itdadao&utm_medium=referral
     *
     * @param handler handler of the follower
     * @return last proposed zxid
     */
    synchronized public long startForwarding(LearnerHandler handler, long lastSeenZxid) {
        // Queue up any outstanding requests enabling the receipt of
        // new requests
        LOG.info("lastProposed :" + lastProposed +", lastSeenZxid:" + lastSeenZxid + ", handler:" + handler);
        if (lastProposed > lastSeenZxid) {
            LOG.info("toBeApplied :" + toBeApplied);
            for (Proposal p : toBeApplied) {
                if (p.packet.getZxid() <= lastSeenZxid) {
                    continue;
                }
                handler.queuePacket(p.packet);
                // Since the proposal has been committed we need to send the
                // commit message also
                QuorumPacket qp = new QuorumPacket(Leader.COMMIT, p.packet.getZxid(), null, null);
                LOG.info("qp :" + qp);
                handler.queuePacket(qp);
            }
            // Only participant need to get outstanding proposals
            if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
                LOG.info("outstandingProposals : " + outstandingProposals);
                List<Long>zxids = new ArrayList<Long>(outstandingProposals.keySet());
                Collections.sort(zxids);
                for (Long zxid: zxids) {
                    if (zxid <= lastSeenZxid) {
                        continue;
                    }
                    handler.queuePacket(outstandingProposals.get(zxid).packet);
                }
            }
        }
        LOG.info("handler:" + handler);
        if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
            addForwardingFollower(handler);
        } else {
            addObserverLearnerHandler(handler);
        }
                
        return lastProposed;
    }

    private HashSet<Long> connectingFollowers = new HashSet<Long>();

    /**
     * 这里的方法先 leader 进行调用, 然后 LeaderHandler 里面也进行调用
     */
    public long getEpochToPropose(long sid, long lastAcceptedEpoch) throws InterruptedException, IOException {
        LOG.info("getEpochToPropose myid:" + sid + ", lastAcceptedEpoch:"+lastAcceptedEpoch +", connectingFollowers:" + connectingFollowers + ", waitingForNewEpoch:"+waitingForNewEpoch +", epoch:"+epoch);
        synchronized(connectingFollowers) {
            if (!waitingForNewEpoch) {
                return epoch;
            }
            if (lastAcceptedEpoch >= epoch) {
                epoch = lastAcceptedEpoch+1;
            }
            connectingFollowers.add(sid);                           // 将自己加入到 connectingFollowers, 后续会判断 connectingFollowers.contains

            LOG.info("getEpochToPropose myid:" + sid + ", lastAcceptedEpoch:" + lastAcceptedEpoch + ", connectingFollowers:" + connectingFollowers + ", waitingForNewEpoch:" + waitingForNewEpoch + ", epoch:" + epoch);

            QuorumVerifier verifier = self.getQuorumVerifier();
            if (connectingFollowers.contains(self.getId()) &&      // 自己已经投票, 并且 投票中已经满足过半的原则, 然后就进行唤醒下面代码中的 wait 等待 (connectingFollowers.wait(end - cur))
                                            verifier.containsQuorum(connectingFollowers)) { // 判断投票是否满足 过半的原则
                LOG.info("getEpochToPropose myid:" + sid + ", lastAcceptedEpoch:"+lastAcceptedEpoch +
                        ", connectingFollowers:" + connectingFollowers + ", waitingForNewEpoch:"+waitingForNewEpoch +", epoch:"+epoch);
                waitingForNewEpoch = false;
                self.setAcceptedEpoch(epoch);
                connectingFollowers.notifyAll();
            } else {                                                  // 当过半原则没满足时, 则进行相应的等待
                long start = System.currentTimeMillis();
                long cur = start;
                long end = start + self.getInitLimit()*self.getTickTime();
                while(waitingForNewEpoch && cur < end) {            // QuorumPeer 将在此处进行等待, 为的是 Leader 选举的过半原则
                    connectingFollowers.wait(end - cur);
                    cur = System.currentTimeMillis();
                }
                if (waitingForNewEpoch) {                           // 超时, 重新发起选举, 在 QuorumPeer.run.while (running) {} 里面 一直进行选举
                    throw new InterruptedException("Timeout while waiting for epoch from quorum");        
                }
            }
            LOG.info("getEpochToPropose myid:" + sid + ", lastAcceptedEpoch:"+lastAcceptedEpoch +", connectingFollowers:" + connectingFollowers
                    + ", waitingForNewEpoch:"+waitingForNewEpoch +", epoch:"+epoch);
            return epoch;
        }
    }

    private HashSet<Long> electingFollowers = new HashSet<Long>();
    private boolean electionFinished = false;
    public void waitForEpochAck(long id, StateSummary ss) throws IOException, InterruptedException {
        LOG.info("waitForEpochAck :" + id + ", ss :" + ss + ", electingFollowers:" + electingFollowers + ", electionFinished:" + electionFinished);
        LOG.info("electingFollowers:" + electingFollowers);
        LOG.info("electionFinished:" + electionFinished);

        synchronized(electingFollowers) {
            if (electionFinished) {
                return;
            }
            if (ss.getCurrentEpoch() != -1) {
                LOG.info("leaderStateSummary:" + leaderStateSummary);

                boolean isMoreRecentThan = ss.isMoreRecentThan(leaderStateSummary);
                LOG.info("isMoreRecentThan :" + isMoreRecentThan + ", ss:"+ ss +", leaderStateSummary:" + leaderStateSummary);
                if (isMoreRecentThan) {
                    throw new IOException("Follower is ahead of the leader, leader summary: " 
                                                    + leaderStateSummary.getCurrentEpoch()
                                                    + " (current epoch), "
                                                    + leaderStateSummary.getLastZxid()
                                                    + " (last zxid)");
                }
                electingFollowers.add(id);                                    // 将返回 Leader.ACKEPOCH 的 myid 加入到 集合 electingFollowers 里面
                LOG.info("electingFollowers:" + electingFollowers);
            }
            QuorumVerifier verifier = self.getQuorumVerifier();               // 判断是否满足过半的原则(并且自己已经参与其中), 不然额话就进行相应时间的等待, 等待超时的话, 就进行下一轮的 Leader 选举

            LOG.info("electingFollowers:" + electingFollowers + ", self.getId():" + self.getId());
            if (electingFollowers.contains(self.getId()) && verifier.containsQuorum(electingFollowers)) {
                electionFinished = true;
                electingFollowers.notifyAll();
                LOG.info("waitForEpochAck :" + id + ", ss :" + ss + ", electingFollowers:" + electingFollowers + ", electionFinished:" + electionFinished);
            } else {                
                long start = System.currentTimeMillis();
                long cur = start;
                long end = start + self.getInitLimit()*self.getTickTime();
                while(!electionFinished && cur < end) {
                    electingFollowers.wait(end - cur);
                    cur = System.currentTimeMillis();
                }
                LOG.info("waitForEpochAck :" + id + ", ss :" + ss + ", electingFollowers:" + electingFollowers + ", electionFinished:" + electionFinished);
                if (!electionFinished) {
                    throw new InterruptedException("Timeout while waiting for epoch to be acked by quorum");
                }
            }
        }
    }

    /**
     * Return a list of sid in set as string  
     */
    private String getSidSetString(Set<Long> sidSet) {
        StringBuilder sids = new StringBuilder();
        Iterator<Long> iter = sidSet.iterator();
        while (iter.hasNext()) {
            sids.append(iter.next());
            if (!iter.hasNext()) {
              break;
            }
            sids.append(",");
        }
        return sids.toString();
    }

    /**
     * Start up Leader ZooKeeper server and initialize zxid to the new epoch
     */
    // 启动 zookeeper serrver 并 通过 zxid 赋值 epoch
    private synchronized void startZkServer() {
        // Update lastCommitted and Db's zxid to a value representing the new epoch
        lastCommitted = zk.getZxid();
        LOG.info("Have quorum of supporters, sids: [ "
                + getSidSetString(newLeaderProposal.ackSet)
                + " ]; starting up and setting last processed zxid: 0x{}",
                Long.toHexString(zk.getZxid()));
        zk.startup();
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         * 
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        self.updateElectionVote(getEpoch());

        zk.getZKDatabase().setlastProcessedZxid(zk.getZxid());
    }

    /**
     * Process NEWLEADER ack of a given sid and wait until the leader receives
     * sufficient acks.
     *
     * @param sid
     * @param learnerType
     * @throws InterruptedException
     */
    public void waitForNewLeaderAck(long sid, long zxid, LearnerType learnerType) throws InterruptedException {
        LOG.info("sid:" + sid + ", zxid:" + zxid + ", learnerType:" + learnerType);
        synchronized (newLeaderProposal.ackSet) {

            if (quorumFormed) {
                return;
            }

            long currentZxid = newLeaderProposal.packet.getZxid();
            LOG.info("currentZxid:" + currentZxid);
            if (zxid != currentZxid) {
                LOG.error("NEWLEADER ACK from sid: " + sid
                        + " is from a different epoch - current 0x"
                        + Long.toHexString(currentZxid) + " receieved 0x"
                        + Long.toHexString(zxid));
                return;
            }
            LOG.info("learnerType:" + learnerType + ", newLeaderProposal:" + newLeaderProposal);
            if (learnerType == LearnerType.PARTICIPANT) {
                newLeaderProposal.ackSet.add(sid);
            }
            LOG.info("newLeaderProposal.ackSet:" + newLeaderProposal.ackSet);
            if (self.getQuorumVerifier().containsQuorum(newLeaderProposal.ackSet)) {

                LOG.info("newLeaderProposal.ackSet:" + newLeaderProposal.ackSet);
                quorumFormed = true;
                newLeaderProposal.ackSet.notifyAll();
            } else {
                long start = System.currentTimeMillis();
                long cur = start;
                long end = start + self.getInitLimit() * self.getTickTime();
                while (!quorumFormed && cur < end) {
                    newLeaderProposal.ackSet.wait(end - cur);
                    cur = System.currentTimeMillis();
                }
                LOG.info("newLeaderProposal.ackSet:" + newLeaderProposal.ackSet + ", quorumFormed:" + quorumFormed);
                if (!quorumFormed) {
                    throw new InterruptedException(
                            "Timeout while waiting for NEWLEADER to be acked by quorum");
                }
            }
        }
    }

    /**
     * Get string representation of a given packet type
     * @param packetType
     * @return string representing the packet type
     */
    public static String getPacketType(int packetType) {
        switch (packetType) {
        case DIFF:
            return "DIFF";
        case TRUNC:
            return "TRUNC";
        case SNAP:
            return "SNAP";
        case OBSERVERINFO:
            return "OBSERVERINFO";
        case NEWLEADER:
            return "NEWLEADER";
        case FOLLOWERINFO:
            return "FOLLOWERINFO";
        case UPTODATE:
            return "UPTODATE";
        case LEADERINFO:
            return "LEADERINFO";
        case ACKEPOCH:
            return "ACKEPOCH";
        case REQUEST:
            return "REQUEST";
        case PROPOSAL:
            return "PROPOSAL";
        case ACK:
            return "ACK";
        case COMMIT:
            return "COMMIT";
        case PING:
            return "PING";
        case REVALIDATE:
            return "REVALIDATE";
        case SYNC:
            return "SYNC";
        case INFORM:
            return "INFORM";
        default:
            return "UNKNOWN";
        }
    }
}

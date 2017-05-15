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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * This class has the control logic for the Follower.
 */
public class Follower extends Learner{

    private long lastQueued;
    // This is the same object as this.zk, but we cache the downcast op
    final FollowerZooKeeperServer fzk;
    
    Follower(QuorumPeer self,FollowerZooKeeperServer zk) {
        this.self = self;
        this.zk=zk;
        this.fzk = zk;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Follower ").append(sock);
        sb.append(" lastQueuedZxid:").append(lastQueued);
        sb.append(" pendingRevalidationCount:")
            .append(pendingRevalidations.size());
        return sb.toString();
    }

    /**
     * the main method called by the follower to follow the leader
     *
     * @throws InterruptedException
     */
    void followLeader() throws InterruptedException {
        self.end_fle = System.currentTimeMillis();
        LOG.info("FOLLOWING - LEADER ELECTION TOOK - " + (self.end_fle - self.start_fle));
        self.start_fle = 0;
        self.end_fle = 0;
        fzk.registerJMX(new FollowerBean(this, zk), self.jmxLocalPeerBean);
        try {
            InetSocketAddress addr = findLeader();                                  // 根据当前的 QuorumPeer 的 Vote 获取 leader.myid, 从而获取其对应的 Leader.port
            try {
                connectToLeader(addr);                                              // 连接 Leader
                long newEpochZxid = registerWithLeader(Leader.FOLLOWERINFO);     // 与 leader 同步选举的 epoch (This message type is sent by a follower to pass the last zxid)
                LOG.info("newEpochZxid:" + newEpochZxid);

                //check to see if the leader zxid is lower than ours
                //this should never happen but is just a safety check
                long newEpoch = ZxidUtils.getEpochFromZxid(newEpochZxid);           // 返回 leader 的 zxid
                LOG.info("newEpoch :" + newEpoch);
                if (newEpoch < self.getAcceptedEpoch()) {
                    LOG.error("Proposed leader epoch " + ZxidUtils.zxidToString(newEpochZxid)
                            + " is less than our accepted epoch " + ZxidUtils.zxidToString(self.getAcceptedEpoch()));
                    throw new IOException("Error: Epoch of leader is lower");
                }
                LOG.info("syncWithLeader :" + newEpochZxid);
                syncWithLeader(newEpochZxid);                                       // 与 leader 进行数据同步
                QuorumPacket qp = new QuorumPacket();
                while (self.isRunning()) {                                        // 获取 leader 发来的消息, 并进行相应处理, 线程一直在这边循环
                    readPacket(qp);                                                 // 对应的 leader 方, 消息的发送是通过 LearnerHandler.sendPackets
                    LOG.info("qp:" + qp);
                    processPacket(qp);                                             // 处理 Leader 发送过来的数据包
                }
            } catch (IOException e) {
                LOG.warn("Exception when following the leader", e);
                try {
                    sock.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
    
                // clear pending revalidations
                pendingRevalidations.clear();
            }
        } finally {
            zk.unregisterJMX((Learner)this);
        }
    }

    /**
     * Examine the packet received in qp and dispatch based on its contents.
     * @param qp
     * @throws IOException
     */
    protected void processPacket(QuorumPacket qp) throws IOException{
        switch (qp.getType()) {
        case Leader.PING:                                                       // 处理Leader 发来的PING 包(这里有个注意点, 就是 每次 PING 都是 Leader.lead 里面进行主动发送PING, 而对应的 FOLLOWER 也是会进行 PING 的额回复 ), Follower 会将 自己的 session 信息发送给 Leader, 让 Leader 来进行校验 sessionId 是否超时
            ping(qp);            
            break;
        case Leader.PROPOSAL:                                                  // 处理 Leader 发来的 Proposal 包, 投票处理
            TxnHeader hdr = new TxnHeader();
            Record txn = SerializeUtils.deserializeTxn(qp.getData(), hdr);      // 反序列化出 Request
            if (hdr.getZxid() != lastQueued + 1) {                            // 这里说明什么呢, 说明 Follower 可能少掉了 Proposal
                LOG.warn("Got zxid 0x"
                        + Long.toHexString(hdr.getZxid())
                        + " expected 0x"
                        + Long.toHexString(lastQueued + 1));
            }
            lastQueued = hdr.getZxid();
            fzk.logRequest(hdr, txn);                                           // 将 Request 交给 FollowerZooKeeperServer 来进行处理
            break;
        case Leader.COMMIT:                                                    // 处理 Leader 发来的 COMMIT 包投票处理 Proposal 在 Follower 上进行 Proposal 提交
            fzk.commit(qp.getZxid());
            break;
        case Leader.UPTODATE:
            LOG.error("Received an UPTODATE message after Follower started");
            break;
        case Leader.REVALIDATE:
            revalidate(qp);
            break;
        case Leader.SYNC:
            fzk.sync();
            break;
        }
    }

    /**
     * The zxid of the last operation seen
     * @return zxid
     */
    public long getZxid() {
        try {
            synchronized (fzk) {
                return fzk.getZxid();
            }
        } catch (NullPointerException e) {
            LOG.warn("error getting zxid", e);
        }
        return -1;
    }
    
    /**
     * The zxid of the last operation queued
     * @return zxid
     */
    protected long getLastQueued() {
        return lastQueued;
    }

    @Override
    public void shutdown() {    
        LOG.info("shutdown called", new Exception("shutdown Follower"));
        super.shutdown();
    }
}

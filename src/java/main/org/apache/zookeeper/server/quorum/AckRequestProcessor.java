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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;


/**
 *
 * PreRequestProcessor -> ProposalRequestProcessor -> CommitProcessor -> ToBeAppliedRequestProcessor -> FinalRequestProcessor
 *                                                 \_
 *                                                    SyncRequestProcessor -> AckRequestProcessor
 *
 * 主要负责将 SyncRequestProcessor 处理完的事务日志后, Leader 向 Proposal 的投票收集器 发送 ack 反馈
 *                                              (就是 Leader 自己提出 Proposal, 自己通过 AckRequestProcessor 调用 leader.processAck 来说明: leader 自己赞成 这个 Proposal
 * This is a very simple RequestProcessor that simply forwards a request from a
 * previous stage to the leader as an ACK.
 */
class AckRequestProcessor implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AckRequestProcessor.class);
    Leader leader;

    AckRequestProcessor(Leader leader) {
        this.leader = leader;
    }

    /**
     * Forward the request as an ACK to the leader
     */
    public void processRequest(Request request) {
        QuorumPeer self = leader.self;
        if(self != null)                            // Leader 自己提出 Proposal, 自己通过 AckRequestProcessor 调用 leader.processAck 来说明: leader 自己赞成 这个 Proposal
            leader.processAck(self.getId(), request.zxid, null);
        else
            LOG.error("Null QuorumPeer");
    }

    public void shutdown() {
        // XXX No need to do anything
    }
}

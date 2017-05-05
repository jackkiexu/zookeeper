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

import java.util.ArrayList;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;

/**
 * 参考资料
 * http://blog.csdn.net/vinowan/article/details/22197385
 * http://www.aboutyun.com/thread-10287-1-1.html
 *
 * This RequestProcessor matches the incoming committed requests with the
 * locally submitted requests. The trick is that locally submitted requests that
 * change the state of the system will come back as incoming committed requests,
 * so we need to match them up.
 */
public class CommitProcessor extends Thread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessor.class);

    /**
     * Requests that we are holding until the commit comes in.
     */
    // 等待 ACK 确认的 Request
    LinkedList<Request> queuedRequests = new LinkedList<Request>();

    /**
     * Requests that have been committed.
     */
    // 已经 Proposal ACK 过半确认过的 Request, 一般的要么是 Leader 自己 commit, 要么就是 Follower 接收到 Leader 的 commit 消息
    LinkedList<Request> committedRequests = new LinkedList<Request>();

    RequestProcessor nextProcessor;
    // 等待被 nextProcessor 处理的队列, 其里面的数据是从 committedRequests, queuedRequests 里面获取来的
    ArrayList<Request> toProcess = new ArrayList<Request>();

    /**
     * This flag indicates whether we need to wait for a response to come back from the
     * leader or we just let the sync operation flow through like a read. The flag will
     * be true if the CommitProcessor is in a Leader pipeline.
     */
    boolean matchSyncs;

    public CommitProcessor(RequestProcessor nextProcessor, String id, boolean matchSyncs) {
        super("CommitProcessor:" + id);
        this.nextProcessor = nextProcessor;
        this.matchSyncs = matchSyncs;
    }

    volatile boolean finished = false;

    @Override
    public void run() {
        try {
            Request nextPending = null;            
            while (!finished) {                                                             // while loop
                int len = toProcess.size();
                for (int i = 0; i < len; i++) {
                    nextProcessor.processRequest(toProcess.get(i));                       // 将 ack 过半的 Request 丢给 ToBeAppliedRequestProcessor 来进行处理
                }
                toProcess.clear();
                synchronized (this) {
                    if ((queuedRequests.size() == 0 || nextPending != null)               // 如果没有 Commit 的请求, 则进行wait, 直到 commit 请求的到来
                            && committedRequests.size() == 0) {
                        wait();
                        continue;
                    }
                    // First check and see if the commit came in for the pending
                    // request
                    if ((queuedRequests.size() == 0 || nextPending != null)              // 有 commit request 进来, 获取 commit request
                            && committedRequests.size() > 0) {
                        Request r = committedRequests.remove();
                        /*
                         * We match with nextPending so that we can move to the
                         * next request when it is committed. We also want to
                         * use nextPending because it has the cnxn member set
                         * properly.
                         */
                        if (nextPending != null                                             // 这里其实就是比较 nextPending 与 committedRequests 中的 request 请求
                                && nextPending.sessionId == r.sessionId                   // 而 nextPending 又是从 queuedRequests 里面拿出来的, 若相同, 则直接用 committedRequests 里面的 消息头, 消息体, zxid
                                && nextPending.cxid == r.cxid) {
                            // we want to send our version of the request.
                            // the pointer to the connection in the request
                            nextPending.hdr = r.hdr;
                            nextPending.txn = r.txn;
                            nextPending.zxid = r.zxid;
                            toProcess.add(nextPending);                                    // 将 请求 直接加入 toProcess, 直到下次 loop 被 nextProcessor 处理
                            nextPending = null;
                        } else {                                                            // Leader 直接 调用 commit 方法提交的 请求, 直接加入 toProcess, 直到下次 loop 被 nextProcessor 处理
                            // this request came from someone else so just
                            // send the commit packet
                            toProcess.add(r);
                        }
                    }
                }

                // We haven't matched the pending requests, so go back to
                // waiting
                if (nextPending != null) {
                    continue;
                }

                synchronized (this) {
                    // Process the next requests in the queuedRequests
                    while (nextPending == null && queuedRequests.size() > 0) {
                        Request request = queuedRequests.remove();
                        switch (request.type) {
                        case OpCode.create:
                        case OpCode.delete:
                        case OpCode.setData:
                        case OpCode.multi:
                        case OpCode.setACL:
                        case OpCode.createSession:
                        case OpCode.closeSession:
                            nextPending = request;                                            // 若请求是事务请求, 则将 follower 自己提交的 request 赋值给 nextPending
                            break;
                        case OpCode.sync:
                            if (matchSyncs) {
                                nextPending = request;
                            } else {
                                toProcess.add(request);
                            }
                            break;
                        default:
                            toProcess.add(request);
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted exception while waiting", e);
        } catch (Throwable e) {
            LOG.error("Unexpected exception causing CommitProcessor to exit", e);
        }
        LOG.info("CommitProcessor exited loop!");
    }

    synchronized public void commit(Request request) {
        LOG.info("request:" + request + ", finished :" + finished);
        if (!finished) {
            if (request == null) {
                LOG.warn("Committed a null!",
                         new Exception("committing a null! "));
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Committing request:: " + request);
            }
            committedRequests.add(request);
            notifyAll();
        }
    }

    synchronized public void processRequest(Request request) {
        // request.addRQRec(">commit");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }
        
        if (!finished) {
            queuedRequests.add(request);
            notifyAll();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        synchronized (this) {
            finished = true;
            queuedRequests.clear();
            notifyAll();
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

}

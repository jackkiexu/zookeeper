/**
 * zookeeper 资料
 *
 * http://blog.csdn.net/vinowan/article/details/22197049
 *
 * http://blog.csdn.net/fei33423/article/details/53749138
 * 下面这个非常好
 * http://www.aboutyun.com/thread-10287-1-1.html
 *
 * Leader/Follower初始化
 * http://www.aboutyun.com/thread-10286-1-1.html
 *
 * Zookeeper 集群常见问题
 *
 * 0. Follower 上创建 session, 如何通知 Leader ?
 *       FinalRequestProcessor -> zks.processTxn 进行 session 的操作
 *
 * 1. 既然 session 将都由 Leader 来进行校验是否超时, 那超时了怎么办 ?
 *       在 Follower 对应 session 只存储 在 sessionsWithTimeouts, touchTable 里面
 *
 * 1.1 session 在 Leader 检测时超时了, 如何通知客户端
 *
 * 2. SyncLimitCheck 的作用 ?
 *
 * 3. outstandingChanges, outstandingChangesForPath 的作用 ?
 *
 * 4. Leader.toBeApplied Leader.outstandingProposals 的作用 ?
 *
 * Created by xjk on 5/3/17.
 */
package org.apache;
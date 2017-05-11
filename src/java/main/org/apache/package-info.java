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
 * 1. 既然 session 将都由 Leader 来进行校验是否超时, 那超时了怎么办 ?
 * 2. SyncLimitCheck 的作用 ?
 * 3. outstandingChanges, outstandingChangesForPath 的作用 ?
 * 4. Leader.toBeApplied Leader.outstandingProposals 的作用 ?
 *
 * Created by xjk on 5/3/17.
 */
package org.apache;
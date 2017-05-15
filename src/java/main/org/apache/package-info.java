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
 * ZooKeeper 权限控制
 * http://www.wuzesheng.com/?p=2438
 *
 * Zookeeper 集群常见问题
 *
 * 0. Follower 上创建 session, 如何通知 Leader
 *       FinalRequestProcessor -> zks.processTxn 进行 session 的操作
 *
 * 1. 既然 session 将都由 Leader 来进行校验是否超时, 那超时了怎么办
 *       在 Follower 对应 session 只存储 在 sessionsWithTimeouts, touchTable 里面
 *
 * 1.1 session 在 Leader 检测时超时了, 如何通知客户端 ?
 *
 * 2. SyncLimitCheck 的作用
 *      用于检测 Learner 是否按时回复 ACK 给 Leader
 *
 * 3. outstandingChanges, outstandingChangesForPath 的作用
 *      OK , 缓存 DataNode 的信息, 为下次针对相同 PATH 做事务操作做准备, 改变数据做准备, 这里可能隐藏一个细节(就是可能 Leader 在收到过半 ACK 后, 进行 commit 通知所有的 Follower, 而自己在 本机上 commit Request 请求时, 进程被杀掉)
 *
 * 4. Leader.toBeApplied Leader.outstandingProposals 的作用
 *      存储 Leader 自己需要处理的 Proposal, 最终在 FinalRequestProcessor 中进行删除,
 *
 * 5. zookeeper 权限控制
 *      设计 SASL, jaas (http://tetsu.iteye.com/blog/82627)
 *
 * 6. JMX 在 zookeeper 中的使用
 *      具体的实现就在 MBeanRegistry 里面
 *
 * 6.1 Leader.pendingSyncs 的原理 ?
 *
 * 7. zookeeper 分布式锁, 分布式队列, leader 选举设计过程
 *     curator 学习资料 (http://colobu.com/tags/Curator/)
 *     curator (http://colobu.com/tags/Curator/)
 *
 *
 * Created by xjk on 5/3/17.
 */
package org.apache;
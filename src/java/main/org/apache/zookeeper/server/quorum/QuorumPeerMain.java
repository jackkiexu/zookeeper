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

import java.io.File;
import java.io.IOException;

import javax.management.JMException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 *
 * <h2>Configuration file</h2>
 *
 * When the main() method of this class is used to start the program, the first
 * argument is used as a path to the config file, which will be used to obtain
 * configuration information. This file is a Properties file, so keys and
 * values are separated by equals (=) and the key/value pairs are separated
 * by new lines. The following is a general summary of keys used in the
 * configuration file. For full details on this see the documentation in
 * docs/index.html
 * <ol>
 * <li>dataDir - The directory where the ZooKeeper data is stored.</li>
 * <li>dataLogDir - The directory where the ZooKeeper transaction log is stored.</li>
 * <li>clientPort - The port used to communicate with clients.</li>
 * <li>tickTime - The duration of a tick in milliseconds. This is the basic
 * unit of time in ZooKeeper.</li>
 * <li>initLimit - The maximum number of ticks that a follower will wait to
 * initially synchronize with a leader.</li>
 * <li>syncLimit - The maximum number of ticks that a follower will wait for a
 * message (including heartbeats) from the leader.</li>
 * <li>server.<i>id</i> - This is the host:port[:port] that the server with the
 * given id will use for the quorum protocol.</li>
 * </ol>
 * In addition to the config file. There is a file in the data directory called
 * "myid" that contains the server id as an ASCII decimal value.
 *
 */
public class QuorumPeerMain {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerMain.class);

    private static final String USAGE = "Usage: QuorumPeerMain configfile";

    protected QuorumPeer quorumPeer;

    /**
     * To start the replicated server specify the configuration file name on
     * the command line.
     * @param args path to the configfile
     */
    public static void main(String[] args) {
        QuorumPeerMain main = new QuorumPeerMain();
        try {
            main.initializeAndRun(args);
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            System.exit(2);
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            System.exit(2);
        } catch (Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            System.exit(1);
        }
        LOG.info("Exiting normally");
        System.exit(0);
    }

    protected void initializeAndRun(String[] args)
        throws ConfigException, IOException
    {
        QuorumPeerConfig config = new QuorumPeerConfig();
        if (args.length == 1) {
            config.parse(args[0]);
        }

        // Start and schedule the the purge task
        // 下面是一个进行定时清除 snapshot 文件的定时任务, 比较简单, 就是一个 Timer
        DatadirCleanupManager purgeMgr = new DatadirCleanupManager(config
                .getDataDir(), config.getDataLogDir(), config
                .getSnapRetainCount(), config.getPurgeInterval());
        purgeMgr.start();

        if (args.length == 1 && config.servers.size() > 0) {
            runFromConfig(config);
        } else {
            LOG.warn("Either no config or no quorum defined in config, running "
                    + " in standalone mode");
            // there is only server in the quorum -- run as standalone
            ZooKeeperServerMain.main(args);
        }
    }

    // 根据 配置 QuorumPeerConfig 来启动  QuorumPeer
    public void runFromConfig(QuorumPeerConfig config) throws IOException {
        LOG.info("QuorumPeerConfig : " + config);
      try {
          ManagedUtil.registerLog4jMBeans();
      } catch (JMException e) {
          LOG.warn("Unable to register log4j JMX control", e);
      }
  
      LOG.info("Starting quorum peer");
      try {                                                                         // 1. 在 ZooKeeper 集群中, 每个 QuorumPeer 代表一个 服务
          ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
          cnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns());
  
          quorumPeer = new QuorumPeer();
          quorumPeer.setClientPortAddress(config.getClientPortAddress());
          quorumPeer.setTxnFactory(new FileTxnSnapLog(                              // 2. 设置 FileTxnSnapLog(这个类包裹 TxnLog, SnapShot)
                  new File(config.getDataLogDir()),
                  new File(config.getDataDir())));
          quorumPeer.setQuorumPeers(config.getServers());                           // 3. 集群中所有机器
          quorumPeer.setElectionType(config.getElectionAlg());                      // 4. 设置集群 Leader 选举所使用的的算法(默认值 3, 代表 FastLeaderElection)
          quorumPeer.setMyid(config.getServerId());                                 // 5. 每个 QuorumPeer 设置一个 myId 用于区分集群中的各个节点
          quorumPeer.setTickTime(config.getTickTime());
          quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());           // 6. 客户端最小的 sessionTimeout 时间(若不设置的话, 就是 tickTime * 2)
          quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());           // 7. 客户端最小的 sessionTimeout 时间(若不设置的话, 就是 tickTime * 20)
          quorumPeer.setInitLimit(config.getInitLimit());                           // 8. 最常用的就是 initLimit * tickTime, getEpochToPropose(等待集群中所有节点的 Epoch值 ) waitForEpochAck(在 Leader 建立过程中, Leader 会向所有节点发送 LEADERINFO, 而Follower 节点会回复ACKEPOCH) waitForNewLeaderAck(在 Leader 建立的过程中, Leader 会向 Follower 发送 NEWLEADER, waitForNewLeaderAck 就是等候所有Follower 回复对应的 ACK 值)
          quorumPeer.setSyncLimit(config.getSyncLimit());                           // 9. 常用方法 self.tickTime * self.syncLimit 用于限制集群中各个节点相互连接的 socket 的soTimeout
          quorumPeer.setQuorumVerifier(config.getQuorumVerifier());                 // 10.投票方法, 默认超过半数就通过 (默认值 QuorumMaj)
          quorumPeer.setCnxnFactory(cnxnFactory);                                   // 11.设置集群节点接收client端连接使用的 nioCnxnFactory(用 基于原生 java nio, netty nio) (PS 在原生 NIO 的类中发现代码中没有处理 java nio CPU 100% 的bug)
          quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));     // 12.设置 ZKDataBase
          quorumPeer.setLearnerType(config.getPeerType());                          // 13.设置节点的类别 (参与者/观察者)
          quorumPeer.setSyncEnabled(config.getSyncEnabled());                       // 14.这个参数主要用于 (Observer Enables/Disables sync request processor. This option is enable by default and is to be used with observers.) 就是 Observer 是否使用 SyncRequestProcessor
          quorumPeer.setQuorumListenOnAllIPs(config.getQuorumListenOnAllIPs());
  
          quorumPeer.start();                                                       // 15.开启服务
          LOG.info("quorumPeer.join begin");
          quorumPeer.join();                                                        // 16.等到 线程 quorumPeer 执行完成, 程序才会继续向下再执行, 详情见方法注解 (Waits for this thread to die.)
          LOG.info("quorumPeer.join end");
      } catch (InterruptedException e) {
          // warn, but generally this is ok
          LOG.warn("Quorum Peer interrupted", e);
      }
    }
}

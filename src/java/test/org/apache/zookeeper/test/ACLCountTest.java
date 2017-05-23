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

package org.apache.zookeeper.test;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.List;

import org.apache.zookeeper.*;
import org.apache.zookeeper.common.PathTrie;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.apache.zookeeper.server.auth.KerberosName;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;

public class ACLCountTest extends ZKTestCase implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(ACLTest.class);
    private static final String HOSTPORT =
            "127.0.0.1:" + PortAssignment.unique();
    private volatile CountDownLatch startSignal;

    /**
     *
     * Create a node and add 4 ACL values to it, but there are only 2 unique ACL values,
     * and each is repeated once:
     *
     *   ACL(ZooDefs.Perms.READ,ZooDefs.Ids.ANYONE_ID_UNSAFE);
     *   ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.AUTH_IDS);
     *   ACL(ZooDefs.Perms.READ,ZooDefs.Ids.ANYONE_ID_UNSAFE);
     *   ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.AUTH_IDS);
     *
     * Even though we've added 4 ACL values, there should only be 2 ACLs for that node,
     * since there are only 2 *unique* ACL values.
     */
    @Test
    public void testAclCount() throws Exception {
        File tmpDir = ClientBase.createTmpDir();
        ClientBase.setupTestEnv();
        // 创建服务端
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 30000000);
        SyncRequestProcessor.setSnapCount(1000);
        final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
        // 创建 服务端连接器
        ServerCnxnFactory f = ServerCnxnFactory.createFactory(PORT, -1);
        // 启动服务端
        f.startup(zks);
        ZooKeeper zk;

        final ArrayList<ACL> CREATOR_ALL_AND_WORLD_READABLE =
                new ArrayList<ACL>() { {
                    add(new ACL(ZooDefs.Perms.READ,ZooDefs.Ids.ANYONE_ID_UNSAFE));
                    add(new ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.AUTH_IDS));
                    add(new ACL(ZooDefs.Perms.READ,ZooDefs.Ids.ANYONE_ID_UNSAFE));
                    add(new ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.AUTH_IDS));
                }};

        try {
            LOG.info("starting up the zookeeper server .. waiting");
            Assert.assertTrue("waiting for server being up", ClientBase.waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT));
            // 启动客户端
            zk = new ZooKeeper(HOSTPORT, CONNECTION_TIMEOUT, this);

            zk.addAuthInfo("digest", "pat:test".getBytes());
            zk.setACL("/", Ids.CREATOR_ALL_ACL, -1);

            String path = "/path";

            try {
                Assert.assertEquals(4,CREATOR_ALL_AND_WORLD_READABLE.size());
            }
            catch (Exception e) {
                LOG.error("Something is fundamentally wrong with ArrayList's add() method. add()ing four times to an empty ArrayList should result in an ArrayList with 4 members.");
                throw e;
            }
            // 创建节点 (赋值权限 + 持久化的节点)
            zk.create(path,path.getBytes(),CREATOR_ALL_AND_WORLD_READABLE,CreateMode.PERSISTENT);
            List<ACL> acls = zk.getACL("/path", new Stat());
            Assert.assertEquals(2,acls.size());
        }
        catch (Exception e) {
            // test failed somehow.
            Assert.assertTrue(false);
        }

        f.shutdown();
        zks.shutdown();
    }


    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatcherEvent)
     */
    public void process(WatchedEvent event) {
        LOG.info("Event:" + event.getState() + " " + event.getType() + " "
                + event.getPath());
        if (event.getState() == KeeperState.SyncConnected) {
            if (startSignal != null && startSignal.getCount() > 0) {
                LOG.info("startsignal.countDown()");
                startSignal.countDown();
            } else {
                LOG.warn("startsignal " + startSignal);
            }
        }
    }

    @Test
    public void testPathTire(){
        PathTrie pTrie = new PathTrie();
        String path2 = "/zookeeper";
        String path3 = "/zookeeper/quota";
        String path4 = "/zookeeper/test";
        String path5 = "/test";

        pTrie.addPath(path2);
        pTrie.addPath(path3);
        pTrie.addPath(path4);
        pTrie.addPath(path5);

        pTrie.findMaxPrefix(path5);
        pTrie.findMaxPrefix(path3);
    }


    @Test
    public void testStringBuilder(){
        StringBuilder path = new StringBuilder("/zookeeper");
        StringBuilder path2 = new StringBuilder("/zookeeper/quota");

        System.out.println("path:" + path2.delete(11, Integer.MAX_VALUE));
    }


    @Test
    public void initializeNextSession(){
        long sessionId = SessionTrackerImpl.initializeNextSession(9l);
        LOG.info("sessionId:" + sessionId);
    }



    @Test
    public void testApp() throws Exception{
        ProviderRegistry.initialize();
        LOG.info("ProviderRegistry.listProviders:" + ProviderRegistry.listProviders());

        LOG.info(DigestAuthenticationProvider.generateDigest("super:superpw"));
    }



    @Test
    public void testSuperServer(){
        List<ACL> acls = new ArrayList<>();

        try{
            Id id1 = new Id("digest", DigestAuthenticationProvider.generateDigest("fish:fishpw"));
            ACL acl1 = new ACL(ZooDefs.Perms.WRITE, id1);

            Id id2 = new Id("digest", DigestAuthenticationProvider.generateDigest("qsd:qsdpw"));
            ACL acl2 = new ACL(ZooDefs.Perms.READ, id2);

            acls.add(acl1);
            acls.add(acl2);
        }catch (Exception e){

        }

        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper("10.0.1.75:2181,10.0.1.76:2181,10.0.1.77:2181", 300000, new Watcher() {
                // 监控所有被触发的事件
                public void process(WatchedEvent event) {
                    System.out.println("已经触发了" + event.getType() + "事件！");
                }
            });
            if (zk.exists("/test", true) == null) {
                System.out.println(zk.create("/test", "ACL测试".getBytes(), acls, CreateMode.PERSISTENT));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSuperClient() {
        try {
            ZooKeeper zk = new ZooKeeper("10.0.1.75:2181,10.0.1.76:2181,10.0.1.77:2181", 300000, new Watcher() {
                // 监控所有被触发的事件
                public void process(WatchedEvent event) {
                    System.out.println("已经触发了" + event.getType() + "事件！");
                }
            });
            zk.addAuthInfo("digest", "super:superpw".getBytes());
            System.out.println(new String(zk.getData("/test", null, null)));
            zk.setData("/test", "I change！".getBytes(), -1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testKerberosName(){
        new KerberosName("owen@foo/bar.com");
        new KerberosName("user@KERB.REALM/server.com");
        new KerberosName("user@KERB.REALM1@KERB.REALM2");

    }

    @Test
    public void testParsing() throws Exception {
        final String principalNameFull = "HTTP/abc.com@EXAMPLE.COM";
        final String principalNameWoRealm = "HTTP/abc.com";
        final String principalNameWoHost = "HTTP@EXAMPLE.COM";

        final KerberosName kerbNameFull = new KerberosName(principalNameFull);
        Assert.assertEquals("HTTP", kerbNameFull.getServiceName());
        Assert.assertEquals("abc.com", kerbNameFull.getHostName());
        Assert.assertEquals("EXAMPLE.COM", kerbNameFull.getRealm());

        final KerberosName kerbNamewoRealm = new KerberosName(principalNameWoRealm);
        Assert.assertEquals("HTTP", kerbNamewoRealm.getServiceName());
        Assert.assertEquals("abc.com", kerbNamewoRealm.getHostName());
        Assert.assertEquals(null, kerbNamewoRealm.getRealm());

        final KerberosName kerbNameWoHost = new KerberosName(principalNameWoHost);
        Assert.assertEquals("HTTP", kerbNameWoHost.getServiceName());
        Assert.assertEquals(null, kerbNameWoHost.getHostName());
        Assert.assertEquals("EXAMPLE.COM", kerbNameWoHost.getRealm());
    }

    @Test
    public void testTxnLog(){
        File file = new File("/Users/xjk/Documents/ideaworkspace/zookeeper/build/2017-05-22-23.dir/version-2");

        File[] files = FileTxnLog.getLogFiles(file.listFiles(), 1089);
        LOG.info("files:" + files);

    }


    @Test
    public void testFileIterator() throws Exception{
        File file = new File("/Users/xjk/Documents/ideaworkspace/zookeeper/build/2017-05-23-23.dir/version-2");
        FileTxnLog.FileTxnIterator fileTxnIterator = new FileTxnLog.FileTxnIterator(file, 0);

    }
}

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

package org.apache.zookeeper.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.jute.Record;
import org.apache.jute.BinaryOutputArchive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.SetACLTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.CheckVersionTxn;
import org.apache.zookeeper.txn.Txn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * This request processor is generally at the start of a RequestProcessor
 * change. It sets up any transactions associated with requests that change the
 * state of the system. It counts on ZooKeeperServer to update
 * outstandingRequests, so that it can take into account transactions that are
 * in the queue to be applied when generating a transaction.
 */
public class PrepRequestProcessor extends Thread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PrepRequestProcessor.class);

    static boolean skipACL;
    static {                                    // 获取配置信息, 是否跳过 权限验证
        skipACL = System.getProperty("zookeeper.skipACL", "no").equals("yes");
        if (skipACL) {
            LOG.info("zookeeper.skipACL==\"yes\", ACL checks will be skipped");
        }
    }

    /**
     * this is only for testing purposes.
     * should never be useed otherwise
     */
    private static  boolean failCreate = false;
    // 已提交请求的队列
    LinkedBlockingQueue<Request> submittedRequests = new LinkedBlockingQueue<Request>();
    // 下个处理器
    RequestProcessor nextProcessor;

    ZooKeeperServer zks;

    public PrepRequestProcessor(ZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        super("ProcessThread(sid:" + zks.getServerId() + " cport:" + zks.getClientPort() + "):");
        this.nextProcessor = nextProcessor;
        this.zks = zks;
    }

    /**
     * method for tests to set failCreate
     * @param b
     */
    public static void setFailCreate(boolean b) {
        failCreate = b;
    }
    @Override
    public void run() {
        try {
            while (true) {
                Request request = submittedRequests.take();                         // 从 queue 里面拿出 request, 没有就阻塞
                LOG.info("request:"+request);
                long traceMask = ZooTrace.CLIENT_REQUEST_TRACE_MASK;
                if (request.type == OpCode.ping) {
                    traceMask = ZooTrace.CLIENT_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logRequest(LOG, traceMask, 'P', request, "");
                }
                if (Request.requestOfDeath == request) {                           // 在关闭处理器后, 会添加 requestOfDeath, 表示关闭处理器后不再接收请求
                    break;
                }
                pRequest(request);                                                 // 对 Request 进行处理
            }
        } catch (InterruptedException e) {                                          // 异常处理
            LOG.error("Unexpected interruption", e);
        } catch (RequestProcessorException e) {
            if (e.getCause() instanceof XidRolloverException) {
                LOG.info(e.getCause().getMessage());
            }
            LOG.error("Unexpected exception", e);
        } catch (Exception e) {
            LOG.error("Unexpected exception", e);
        }
        LOG.info("PrepRequestProcessor exited loop!");
    }

    ChangeRecord getRecordForPath(String path) throws KeeperException.NoNodeException {
        ChangeRecord lastChange = null;
        synchronized (zks.outstandingChanges) {
            lastChange = zks.outstandingChangesForPath.get(path);
            /*
            for (int i = 0; i < zks.outstandingChanges.size(); i++) {
                ChangeRecord c = zks.outstandingChanges.get(i);
                if (c.path.equals(path)) {
                    lastChange = c;
                }
            }
            */
            if (lastChange == null) {
                DataNode n = zks.getZKDatabase().getNode(path);
                if (n != null) {
                    Long acl;
                    Set<String> children;
                    synchronized(n) {
                        acl = n.acl;
                        children = n.getChildren();
                    }
                    lastChange = new ChangeRecord(-1, path, n.stat,
                        children != null ? children.size() : 0,
                            zks.getZKDatabase().convertLong(acl));
                }
            }
        }
        if (lastChange == null || lastChange.stat == null) {
            throw new KeeperException.NoNodeException(path);
        }
        return lastChange;
    }

    void addChangeRecord(ChangeRecord c) {
        synchronized (zks.outstandingChanges) {
            zks.outstandingChanges.add(c);
            zks.outstandingChangesForPath.put(c.path, c);
        }
    }

    /**
     * Grab current pending change records for each op in a multi-op.
     * 
     * This is used inside MultiOp error code path to rollback in the event
     * of a failed multi-op.
     *
     * @param multiRequest
     */
    HashMap<String, ChangeRecord> getPendingChanges(MultiTransactionRecord multiRequest) {
    	HashMap<String, ChangeRecord> pendingChangeRecords = new HashMap<String, ChangeRecord>();
    	
        for(Op op: multiRequest) {
    		String path = op.getPath();

    		try {
    		    ChangeRecord cr = getRecordForPath(path);
    		    if (cr != null) {
    		        pendingChangeRecords.put(path, cr);
    		    }
    		    /*
    		     * ZOOKEEPER-1624 - We need to store for parent's ChangeRecord
    		     * of the parent node of a request. So that if this is a
    		     * sequential node creation request, rollbackPendingChanges()
    		     * can restore previous parent's ChangeRecord correctly.
    		     *
    		     * Otherwise, sequential node name generation will be incorrect
    		     * for a subsequent request.
    		     */
    		    int lastSlash = path.lastIndexOf('/');
    		    if (lastSlash == -1 || path.indexOf('\0') != -1) {
    		        continue;
    		    }
                // 提取节点的父节点路径
    		    String parentPath = path.substring(0, lastSlash);
                // 获取父节点的 record
    		    ChangeRecord parentCr = getRecordForPath(parentPath);
    		    if (parentCr != null) {
    		        pendingChangeRecords.put(parentPath, parentCr);
    		    }
    		} catch (KeeperException.NoNodeException e) {
    			// ignore this one
    		}
    	}
        
        return pendingChangeRecords;
    }

    /**
     * Rollback pending changes records from a failed multi-op.
     *
     * If a multi-op fails, we can't leave any invalid change records we created
     * around. We also need to restore their prior value (if any) if their prior
     * value is still valid.
     *
     * @param zxid
     * @param pendingChangeRecords
     */
    void rollbackPendingChanges(long zxid, HashMap<String, ChangeRecord>pendingChangeRecords) {

        synchronized (zks.outstandingChanges) {
            // Grab a list iterator starting at the END of the list so we can iterate in reverse
            ListIterator<ChangeRecord> iter = zks.outstandingChanges.listIterator(zks.outstandingChanges.size());
            while (iter.hasPrevious()) {
                ChangeRecord c = iter.previous();
                if (c.zxid == zxid) {
                    iter.remove();
                    zks.outstandingChangesForPath.remove(c.path);
                } else {
                    break;
                }
            }
           
            boolean empty = zks.outstandingChanges.isEmpty();
            long firstZxid = 0;
            if (!empty) {
                firstZxid = zks.outstandingChanges.get(0).zxid;
            }

            Iterator<ChangeRecord> priorIter = pendingChangeRecords.values().iterator();
            while (priorIter.hasNext()) {
                ChangeRecord c = priorIter.next();
                 
                /* Don't apply any prior change records less than firstZxid */
                if (!empty && (c.zxid < firstZxid)) {
                    continue;
                }

                zks.outstandingChangesForPath.put(c.path, c);
            }
        }
    }

    public static void checkACL(ZooKeeperServer zks, List<ACL> acl, int perm,
            List<Id> ids) throws KeeperException.NoAuthException {
        if (skipACL) {
            return;
        }
        if (acl == null || acl.size() == 0) {
            return;
        }
        for (Id authId : ids) {
            if (authId.getScheme().equals("super")) {
                return;
            }
        }
        for (ACL a : acl) {
            Id id = a.getId();
            if ((a.getPerms() & perm) != 0) {
                if (id.getScheme().equals("world")
                        && id.getId().equals("anyone")) {
                    return;
                }
                AuthenticationProvider ap = ProviderRegistry.getProvider(id
                        .getScheme());
                if (ap != null) {
                    for (Id authId : ids) {                        
                        if (authId.getScheme().equals(id.getScheme())
                                && ap.matches(authId.getId(), id.getId())) {
                            return;
                        }
                    }
                }
            }
        }
        throw new KeeperException.NoAuthException();
    }

    /**
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param type
     * @param zxid
     * @param request
     * @param record
     * @param deserialize 是否需要从 request.request 里面反序列化出 对应的 Record
     */
    @SuppressWarnings("unchecked")
    protected void pRequest2Txn(int type, long zxid, Request request, Record record, boolean deserialize)
        throws KeeperException, IOException, RequestProcessorException
    {
        request.hdr = new TxnHeader(request.sessionId, request.cxid, zxid,                  // 1. 组装请求事物头 TxnHeader
                                    zks.getTime(), type);

        switch (type) {
            case OpCode.create:                                                                // 创建 path 请求
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());        //  进行校验 session 是否是 createSession 时的 owner
                CreateRequest createRequest = (CreateRequest)record;   
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, createRequest);   // 从 request.request 里面发序列化出 createRequest
                String path = createRequest.getPath();                                          // 获取路径
                int lastSlash = path.lastIndexOf('/');
                if (lastSlash == -1 || path.indexOf('\0') != -1 || failCreate) {               // 校验 path 路径
                    LOG.info("Invalid path " + path + " with session 0x" +
                            Long.toHexString(request.sessionId));
                    throw new KeeperException.BadArgumentsException(path);
                }
                List<ACL> listACL = removeDuplicates(createRequest.getAcl());                   // ACL 去重
                if (!fixupACL(request.authInfo, listACL)) {
                    throw new KeeperException.InvalidACLException(path);
                }
                String parentPath = path.substring(0, lastSlash);                               // 获取父节点
                ChangeRecord parentRecord = getRecordForPath(parentPath);

                checkACL(zks, parentRecord.acl, ZooDefs.Perms.CREATE,                         // 检查 acl
                        request.authInfo);
                int parentCVersion = parentRecord.stat.getCversion();                          // 获取父节点的 子节点创建的版本
                CreateMode createMode =
                    CreateMode.fromFlag(createRequest.getFlags());
                if (createMode.isSequential()) {                                                // 判断是否是 sequential 模式 创建 path
                    path = path + String.format(Locale.ENGLISH, "%010d", parentCVersion);
                }
                try {
                    PathUtils.validatePath(path);                                               // path 校验
                } catch(IllegalArgumentException ie) {
                    LOG.info("Invalid path " + path + " with session 0x" +
                            Long.toHexString(request.sessionId));
                    throw new KeeperException.BadArgumentsException(path);
                }
                try {
                    if (getRecordForPath(path) != null) {
                        throw new KeeperException.NodeExistsException(path);
                    }
                } catch (KeeperException.NoNodeException e) {
                    // ignore this one
                }
                boolean ephemeralParent = parentRecord.stat.getEphemeralOwner() != 0;       // 父节点是否是临时节点 (不允许在临时节点下面创建节点)
                if (ephemeralParent) {
                    throw new KeeperException.NoChildrenForEphemeralsException(path);
                }
                int newCversion = parentRecord.stat.getCversion()+1;                        // 创建子节点的 version ++
                request.txn = new CreateTxn(path, createRequest.getData(),                  // 构建 CreateTxn
                        listACL,
                        createMode.isEphemeral(), newCversion);
                StatPersisted s = new StatPersisted();
                if (createMode.isEphemeral()) {                                             // 判断创建的 path 是否是 临时节点
                    s.setEphemeralOwner(request.sessionId);
                }
                parentRecord = parentRecord.duplicate(request.hdr.getZxid());
                parentRecord.childCount++;                                                // 父 path 的 子节点数 ++
                parentRecord.stat.setCversion(newCversion);                                // 设置新的 cversion
                addChangeRecord(parentRecord);                                              // 添加父节点要改变的事件到对应的队列里面
                addChangeRecord(new ChangeRecord(request.hdr.getZxid(), path, s,           // 将子节点新生成的事件添加到对应的对立里面
                        0, listACL));
                break;
            case OpCode.delete:                                                                         // 删除 path 请求
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());              // 检查 session (1. 过期, 2. 是否属于对应 owner)
                DeleteRequest deleteRequest = (DeleteRequest)record;
                if(deserialize)                                                                          // 反序列化 生成 deleteRequest
                    ByteBufferInputStream.byteBuffer2Record(request.request, deleteRequest);
                path = deleteRequest.getPath();                                                          // 要删除的 path
                lastSlash = path.lastIndexOf('/');
                if (lastSlash == -1 || path.indexOf('\0') != -1
                        || zks.getZKDatabase().isSpecialPath(path)) {
                    throw new KeeperException.BadArgumentsException(path);
                }
                parentPath = path.substring(0, lastSlash);                                              // 获取父节点
                parentRecord = getRecordForPath(parentPath);                                            //
                ChangeRecord nodeRecord = getRecordForPath(path);
                checkACL(zks, parentRecord.acl, ZooDefs.Perms.DELETE,
                        request.authInfo);
                int version = deleteRequest.getVersion();
                if (version != -1 && nodeRecord.stat.getVersion() != version) {
                    throw new KeeperException.BadVersionException(path);
                }
                if (nodeRecord.childCount > 0) {
                    throw new KeeperException.NotEmptyException(path);
                }
                request.txn = new DeleteTxn(path);
                parentRecord = parentRecord.duplicate(request.hdr.getZxid());
                parentRecord.childCount--;
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(request.hdr.getZxid(), path,
                        null, -1, null));
                break;
            case OpCode.setData:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                SetDataRequest setDataRequest = (SetDataRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, setDataRequest);
                path = setDataRequest.getPath();
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.WRITE,
                        request.authInfo);
                version = setDataRequest.getVersion();
                int currentVersion = nodeRecord.stat.getVersion();
                if (version != -1 && version != currentVersion) {
                    throw new KeeperException.BadVersionException(path);
                }
                version = currentVersion + 1;
                request.txn = new SetDataTxn(path, setDataRequest.getData(), version);
                nodeRecord = nodeRecord.duplicate(request.hdr.getZxid());
                nodeRecord.stat.setVersion(version);
                addChangeRecord(nodeRecord);
                break;
            case OpCode.setACL:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                SetACLRequest setAclRequest = (SetACLRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, setAclRequest);
                path = setAclRequest.getPath();
                listACL = removeDuplicates(setAclRequest.getAcl());
                if (!fixupACL(request.authInfo, listACL)) {
                    throw new KeeperException.InvalidACLException(path);
                }
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.ADMIN,
                        request.authInfo);
                version = setAclRequest.getVersion();
                currentVersion = nodeRecord.stat.getAversion();
                if (version != -1 && version != currentVersion) {
                    throw new KeeperException.BadVersionException(path);
                }
                version = currentVersion + 1;
                request.txn = new SetACLTxn(path, listACL, version);
                nodeRecord = nodeRecord.duplicate(request.hdr.getZxid());
                nodeRecord.stat.setAversion(version);
                addChangeRecord(nodeRecord);
                break;
            case OpCode.createSession:                                                         // 创建 session
                request.request.rewind();
                int to = request.request.getInt();
                request.txn = new CreateSessionTxn(to);
                request.request.rewind();
                zks.sessionTracker.addSession(request.sessionId, to);
                zks.setOwner(request.sessionId, request.getOwner());
                break;
            case OpCode.closeSession:
                // We don't want to do this check since the session expiration thread
                // queues up this operation without being the session owner.
                // this request is the last of the session so it should be ok
                //zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                HashSet<String> es = zks.getZKDatabase().getEphemerals(request.sessionId);      // 获取 session 对应的 临时 path
                synchronized (zks.outstandingChanges) {
                    for (ChangeRecord c : zks.outstandingChanges) {
                        if (c.stat == null) {
                            // Doing a delete
                            es.remove(c.path);
                        } else if (c.stat.getEphemeralOwner() == request.sessionId) {
                            es.add(c.path);
                        }
                    }
                    for (String path2Delete : es) {
                        addChangeRecord(new ChangeRecord(request.hdr.getZxid(),
                                path2Delete, null, 0, null));
                    }

                    zks.sessionTracker.setSessionClosing(request.sessionId);
                }

                LOG.info("Processed session termination for sessionid: 0x"
                        + Long.toHexString(request.sessionId));
                break;
            case OpCode.check:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                CheckVersionRequest checkVersionRequest = (CheckVersionRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, checkVersionRequest);
                path = checkVersionRequest.getPath();
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.READ,
                        request.authInfo);
                version = checkVersionRequest.getVersion();
                currentVersion = nodeRecord.stat.getVersion();
                if (version != -1 && version != currentVersion) {
                    throw new KeeperException.BadVersionException(path);
                }
                version = currentVersion + 1;
                request.txn = new CheckVersionTxn(path, version);
                break;
        }
    }

    /**
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param request
     */
    @SuppressWarnings("unchecked")
    protected void pRequest(Request request) throws RequestProcessorException {
        // LOG.info("Prep>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = 0x" + Long.toHexString(request.sessionId));
        request.hdr = null;
        request.txn = null;
        
        try {
            switch (request.type) {
                case OpCode.create:                                                                     // 新建 path 请求
                CreateRequest createRequest = new CreateRequest();
                pRequest2Txn(request.type, zks.getNextZxid(), request, createRequest, true);
                break;
            case OpCode.delete:                                                                         // 删除请求
                DeleteRequest deleteRequest = new DeleteRequest();               
                pRequest2Txn(request.type, zks.getNextZxid(), request, deleteRequest, true);
                break;
            case OpCode.setData:                                                                        // 设置数据请求
                SetDataRequest setDataRequest = new SetDataRequest();                
                pRequest2Txn(request.type, zks.getNextZxid(), request, setDataRequest, true);
                break;
            case OpCode.setACL:                                                                         // 设置权限请求
                SetACLRequest setAclRequest = new SetACLRequest();                
                pRequest2Txn(request.type, zks.getNextZxid(), request, setAclRequest, true);
                break;
            case OpCode.check:                                                                          // 版本检查请求
                CheckVersionRequest checkRequest = new CheckVersionRequest();              
                pRequest2Txn(request.type, zks.getNextZxid(), request, checkRequest, true);
                break;
            case OpCode.multi:                                                                          // 多重请求
                MultiTransactionRecord multiRequest = new MultiTransactionRecord();
                try {                                                                                    // 序列化出 multiRequest
                    ByteBufferInputStream.byteBuffer2Record(request.request, multiRequest);
                } catch(IOException e) {
                   request.hdr =  new TxnHeader(request.sessionId, request.cxid, zks.getNextZxid(),
                            zks.getTime(), OpCode.multi);
                   throw e;
                }
                List<Txn> txns = new ArrayList<Txn>();
                //Each op in a multi-op must have the same zxid!
                long zxid = zks.getNextZxid();
                KeeperException ke = null;

                //Store off current pending change records in case we need to rollback                    // 存储 multiRequest, 可能需要进行回滚
                HashMap<String, ChangeRecord> pendingChanges = getPendingChanges(multiRequest);

                int index = 0;
                for(Op op: multiRequest) {
                    Record subrequest = op.toRequestRecord() ;

                    /* If we've already failed one of the ops, don't bother
                     * trying the rest as we know it's going to fail and it
                     * would be confusing in the logfiles.
                     */
                    if (ke != null) {
                        request.hdr.setType(OpCode.error);
                        request.txn = new ErrorTxn(Code.RUNTIMEINCONSISTENCY.intValue());
                    } 
                    
                    /* Prep the request and convert to a Txn */
                    else {
                        try {                                                                           // 调用 pRequest2Txn 转成事务请求
                            pRequest2Txn(op.getType(), zxid, request, subrequest, false);
                        } catch (KeeperException e) {
                            if (ke == null) {
                                ke = e;
                            }
                            request.hdr.setType(OpCode.error);                                        // 异常处理
                            request.txn = new ErrorTxn(e.code().intValue());
                            LOG.info("Got user-level KeeperException when processing "
                            		+ request.toString() + " aborting remaining multi ops."
                            		+ " Error Path:" + e.getPath()
                            		+ " Error:" + e.getMessage());

                            request.setException(e);

                            /* Rollback change records from failed multi-op */                         // 数据回滚
                            rollbackPendingChanges(zxid, pendingChanges);
                        }
                    }

                    //FIXME: I don't want to have to serialize it here and then
                    //       immediately deserialize in next processor. But I'm 
                    //       not sure how else to get the txn stored into our list.
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                    request.txn.serialize(boa, "request") ;
                    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
                                                                                                    // 将数据请求添加到对应的数组
                    txns.add(new Txn(request.hdr.getType(), bb.array()));
                    index++;
                }
                                                                                                   // 增加事务头
                request.hdr = new TxnHeader(request.sessionId, request.cxid, zxid, zks.getTime(), request.type);
                request.txn = new MultiTxn(txns);                                                 // 事务体
                
                break;

            //create/close session don't require request record
            case OpCode.createSession:                                                          // 创建关闭会话
            case OpCode.closeSession:
                pRequest2Txn(request.type, zks.getNextZxid(), request, null, true);
                break;
 
            //All the rest don't need to create a Txn - just verify session                       // 下面的请求只需要验证会话
            case OpCode.sync:
            case OpCode.exists:
            case OpCode.getData:
            case OpCode.getACL:
            case OpCode.getChildren:
            case OpCode.getChildren2:
            case OpCode.ping:
            case OpCode.setWatches:
                zks.sessionTracker.checkSession(request.sessionId,
                        request.getOwner());
                break;
            }
        } catch (KeeperException e) {
            if (request.hdr != null) {
                request.hdr.setType(OpCode.error);
                request.txn = new ErrorTxn(e.code().intValue());
            }
            LOG.info("Got user-level KeeperException when processing "
                    + request.toString()
                    + " Error Path:" + e.getPath()
                    + " Error:" + e.getMessage());
            request.setException(e);
        } catch (Exception e) {
            // log at error level as we are returning a marshalling
            // error to the user
            LOG.error("Failed to process " + request, e);

            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            if(bb != null){
                bb.rewind();
                while (bb.hasRemaining()) {
                    sb.append(Integer.toHexString(bb.get() & 0xff));
                }
            } else {
                sb.append("request buffer is null");
            }

            LOG.error("Dumping request buffer: 0x" + sb.toString());
            if (request.hdr != null) {
                request.hdr.setType(OpCode.error);
                request.txn = new ErrorTxn(Code.MARSHALLINGERROR.intValue());
            }
        }
        request.zxid = zks.getZxid();                                                            // 这里的 zxid 其实已经 ++ 了
        nextProcessor.processRequest(request);                                                  // 将 request 交由下个处理器处理
    }

    private List<ACL> removeDuplicates(List<ACL> acl) {

        ArrayList<ACL> retval = new ArrayList<ACL>();
        Iterator<ACL> it = acl.iterator();
        while (it.hasNext()) {
            ACL a = it.next();
            if (retval.contains(a) == false) {
                retval.add(a);
            }
        }
        return retval;
    }


    /**
     * This method checks out the acl making sure it isn't null or empty,
     * it has valid schemes and ids, and expanding any relative ids that
     * depend on the requestor's authentication information.
     *
     * @param authInfo list of ACL IDs associated with the client connection
     * @param acl list of ACLs being assigned to the node (create or setACL operation)
     * @return
     */
    private boolean fixupACL(List<Id> authInfo, List<ACL> acl) {
        if (skipACL) {
            return true;
        }
        if (acl == null || acl.size() == 0) {
            return false;
        }

        Iterator<ACL> it = acl.iterator();
        LinkedList<ACL> toAdd = null;
        while (it.hasNext()) {
            ACL a = it.next();
            Id id = a.getId();
            if (id.getScheme().equals("world") && id.getId().equals("anyone")) {
                // wide open
            } else if (id.getScheme().equals("auth")) {
                // This is the "auth" id, so we have to expand it to the
                // authenticated ids of the requestor
                it.remove();
                if (toAdd == null) {
                    toAdd = new LinkedList<ACL>();
                }
                boolean authIdValid = false;
                for (Id cid : authInfo) {
                    AuthenticationProvider ap = ProviderRegistry.getProvider(cid.getScheme());
                    if (ap == null) {
                        LOG.error("Missing AuthenticationProvider for " + cid.getScheme());
                    } else if (ap.isAuthenticated()) {
                        authIdValid = true;
                        toAdd.add(new ACL(a.getPerms(), cid));
                    }
                }
                if (!authIdValid) {
                    return false;
                }
            } else {
                AuthenticationProvider ap = ProviderRegistry.getProvider(id
                        .getScheme());
                if (ap == null) {
                    return false;
                }
                if (!ap.isValid(id.getId())) {
                    return false;
                }
            }
        }
        if (toAdd != null) {
            for (ACL a : toAdd) {
                acl.add(a);
            }
        }
        return acl.size() > 0;
    }

    /**
     * 将需要处理的 request 丢到 队列 submittedRequests 里面,
     * 1. PrepRequestProcessor 其实就是一个不断从 submittedRequests take 出数据, 并且交由 pRequest 来进行处理
     * 2. pRequest 来决定请求 Request 是否需要 pRequest2Txn 来进行处理
     */
    public void processRequest(Request request) {
        // request.addRQRec(">prep="+zks.outstandingChanges.size());
        submittedRequests.add(request);
    }

    public void shutdown() {
        LOG.info("Shutting down");
        submittedRequests.clear();
        submittedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }
}

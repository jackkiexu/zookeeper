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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.util.Enumeration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * This class implements a connection manager for leader election using TCP. It
 * maintains one connection for every pair of servers. The tricky part is to
 * guarantee that there is exactly one connection for every pair of servers that
 * are operating correctly and that can communicate over the network.
 * 
 * If two servers try to start a connection concurrently, then the connection
 * manager uses a very simple tie-breaking mechanism to decide which connection
 * to drop based on the IP addressed of the two parties. 
 * 
 * For every peer, the manager maintains a queue of messages to send. If the
 * connection to any particular peer drops, then the sender thread puts the
 * message back on the list. As this implementation currently uses a queue
 * implementation to maintain messages to send to another peer, we add the
 * message to the tail of the queue, thus changing the order of messages.
 * Although this is not a problem for the leader election, it could be a problem
 * when consolidating peer communication. This is to be verified, though.
 * 
 */

public class QuorumCnxManager {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumCnxManager.class);

    /*
     * Maximum capacity of thread queues
     */
    static final int RECV_CAPACITY = 100;
    // Initialized to 1 to prevent sending
    // stale notifications to peers
    static final int SEND_CAPACITY = 1;

    static final int PACKETMAXSIZE = 1024 * 512; 
    /*
     * Maximum number of attempts to connect to a peer
     */

    static final int MAX_CONNECTION_ATTEMPTS = 2;
    
    /*
     * Negative counter for observer server ids.
     */
    
    private long observerCounter = -1;
    
    /*
     * Connection time out value in milliseconds 
     */
    
    private int cnxTO = 5000;
    
    /*
     * Local IP address
     */
    final QuorumPeer self;

    /*
     * Mapping from Peer to Thread number
     */
    // myid <--> SendWorker (myid 集群中的各个节点的标识, 用于节点之间进行消息发送的线程, 也就是 若集群中有3个节点, 则会存在 3 -> 1, 3 -> 2, 2 -> 1 这样的连接)
    final ConcurrentHashMap<Long, SendWorker> senderWorkerMap;
    // myid <--> queue (myid 集群中的各个节点的标识, queue 本机 QuorumPeer 发送给对应 QuorumPeer 的消息队列)
    final ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>> queueSendMap;
    // myid <--> LastByteBuffer (myid 集群中的各个节点的标识, LastByteBuffer 是与 myid 之间发送的最后一条消息
    final ConcurrentHashMap<Long, ByteBuffer> lastMessageSent;

    /*
     * Reception queue
     */
    public final ArrayBlockingQueue<Message> recvQueue;
    /*
     * Object to synchronize access to recvQueue
     */
    private final Object recvQLock = new Object();

    /*
     * Shutdown flag
     */

    volatile boolean shutdown = false;

    /*
     * Listener thread
     */
    public final Listener listener;

    /*
     * Counter to count worker threads
     */
    // 集群中进行消息发送的线程数
    private AtomicInteger threadCnt = new AtomicInteger(0);

    static public class Message {
        
        Message(ByteBuffer buffer, long sid) {
            this.buffer = buffer;
            this.sid = sid;
        }

        ByteBuffer buffer;
        // 这里的 sid 其实就是 myid
        long sid;

        @Override
        public String toString() {
            return "Message{" +
                    "buffer=" + buffer +
                    ", sid=" + sid +
                    '}';
        }
    }

    public QuorumCnxManager(QuorumPeer self) {
        this.recvQueue = new ArrayBlockingQueue<Message>(RECV_CAPACITY);
        this.queueSendMap = new ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>>();
        this.senderWorkerMap = new ConcurrentHashMap<Long, SendWorker>();
        this.lastMessageSent = new ConcurrentHashMap<Long, ByteBuffer>();
        
        String cnxToValue = System.getProperty("zookeeper.cnxTimeout");
        if(cnxToValue != null){
            this.cnxTO = new Integer(cnxToValue); 
        }
        
        this.self = self;

        // Starts listener thread that waits for connection requests 
        listener = new Listener();
    }

    /**
     * Invokes initiateConnection for testing purposes
     * 
     * @param sid
     */
    public void testInitiateConnection(long sid) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Opening channel to server " + sid);
        }
        Socket sock = new Socket();
        setSockOpts(sock);
        sock.connect(self.getVotingView().get(sid).electionAddr, cnxTO);
        initiateConnection(sock, sid);
    }
    
    /**
     * If this server has initiated the connection, then it gives up on the
     * connection if it loses challenge. Otherwise, it keeps the connection.
     */
    public boolean initiateConnection(Socket sock, Long sid) {
        DataOutputStream dout = null;
        try {
            // Sending id and challenge
            dout = new DataOutputStream(sock.getOutputStream());
            dout.writeLong(self.getId());
            dout.flush();
        } catch (IOException e) {
            LOG.warn("Ignoring exception reading or writing challenge: ", e);
            closeSocket(sock);
            return false;
        }
        
        // If lost the challenge, then drop the new connection
        if (sid > self.getId()) {                                                   // 关键点来了, 只能是高 myid 的服务连接低 myid 的服务 (防止重复建立 socket 连接)
            LOG.info("Have smaller server identifier, so dropping the " +
                     "connection: (" + sid + ", " + self.getId() + ")");
            closeSocket(sock);                                                       // 小 myid 连接 大 myid, 则就进行 socket 关闭
            // Otherwise proceed with the connection
        } else {
            SendWorker sw = new SendWorker(sock, sid);
            RecvWorker rw = new RecvWorker(sock, sid, sw);                          // 在 RecvWorker 里面加上 SendWorker 的引用, 也只是为了关闭的方便
            sw.setRecv(rw);                                                         // 看了一个 这里 setRecv 的作用就是为了一起进行关闭

            SendWorker vsw = senderWorkerMap.get(sid);
            
            if(vsw != null)                                                        // 若以前存在 myid 对应的 SendWorker, 则将 SendWorker 进行关闭
                vsw.finish();
            
            senderWorkerMap.put(sid, sw);                                        // 加入 myid 对应的 SendWorker
            if (!queueSendMap.containsKey(sid)) {
                queueSendMap.put(sid, new ArrayBlockingQueue<ByteBuffer>(        // 理论上来说, 不可能进行这一步操作de
                        SEND_CAPACITY));
            }
            
            sw.start();
            rw.start();
            
            return true;    
            
        }
        return false;
    }

    
    
    /**
     * If this server receives a connection request, then it gives up on the new
     * connection if it wins. Notice that it checks whether it has a connection
     * to this server already or not. If it does, then it sends the smallest
     * possible long value to lose the challenge.
     * 
     */
    public boolean receiveConnection(Socket sock) { // 接收 集群之间各个节点的相互的连接
        Long sid = null;
        LOG.info("sock:"+sock);
        try {
            // Read server id
            DataInputStream din = new DataInputStream(sock.getInputStream());
            sid = din.readLong();   // 读取对应的 myid (这里第一次读取的可能是一个协议的版本号)
            LOG.info("sid:"+sid);
            if (sid < 0) { // this is not a server id but a protocol version (see ZOOKEEPER-1633)
                sid = din.readLong();
                LOG.info("sid:"+sid);
                // next comes the #bytes in the remainder of the message
                int num_remaining_bytes = din.readInt();            // 读取这整条消息的长度
                byte[] b = new byte[num_remaining_bytes];          // 构建要读取数据长度的字节数组
                // remove the remainder of the message from din
                int num_read = din.read(b);                         // 读取消息的内容 (疑惑来了, 这里会不会有拆包断包的情况出现)
                if (num_read != num_remaining_bytes) {              // 数据没有读满, 进行日志记录
                    LOG.error("Read only " + num_read + " bytes out of " + num_remaining_bytes + " sent by server " + sid);
                }
            }
            if (sid == QuorumPeer.OBSERVER_ID) { // 连接过来的是一个观察者
                /*
                 * Choose identifier at random. We need a value to identify
                 * the connection.
                 */
                
                sid = observerCounter--;
                LOG.info("Setting arbitrary identifier to observer: " + sid);
            }
        } catch (IOException e) { // 这里可能会有 EOFException 表示读取数据到文件尾部, 客户端已经断开, 没什么数据可以读取了, 所以直接关闭 socket
            closeSocket(sock);
            LOG.warn("Exception reading or writing challenge: " + e.toString() + ", sock:"+sock);
            return false;
        }
        
        //If wins the challenge, then close the new connection.
        if (sid < self.getId()) {                               // 在集群中为了防止重复连接, 只能允许大的 myid 连接小的
            /*
             * This replica might still believe that the connection to sid is
             * up, so we have to shut down the workers before trying to open a
             * new connection.
             */
            SendWorker sw = senderWorkerMap.get(sid);         // 看看是否已经有 SendWorker, 有的话就进行关闭
            if (sw != null) {
                sw.finish();
            }

            /*
             * Now we start a new connection
             */
            LOG.debug("Create new connection to server: " + sid);
            closeSocket(sock);                                  // 关闭 socket
            connectOne(sid);                                    // 因为自己的 myid 比对方的大, 所以进行主动连接

            // Otherwise start worker threads to receive data.
        } else {                                               // 自己的 myid 比对方小
            SendWorker sw = new SendWorker(sock, sid);         // 建立 SendWorker
            RecvWorker rw = new RecvWorker(sock, sid, sw);     // 建立 RecvWorker
            sw.setRecv(rw);

            SendWorker vsw = senderWorkerMap.get(sid);       // 若以前存在 SendWorker, 则进行关闭
            
            if(vsw != null)
                vsw.finish();
            
            senderWorkerMap.put(sid, sw);
            
            if (!queueSendMap.containsKey(sid)) {           // 若不存在 myid 对应的 消息发送 queue, 则就构建一个
                queueSendMap.put(sid, new ArrayBlockingQueue<ByteBuffer>(
                        SEND_CAPACITY));
            }
            
            sw.start();                                       // 开启 消息发送 及 接收的线程
            rw.start();
            
            return true;    
        }
        return false;
    }

    /**
     * Processes invoke this message to queue a message to send. Currently, 
     * only leader election uses it.
     */
    // leader 选举时会用到

    /**
     * @param sid 这个 sid 就是 在 配置文件中配置的 myid
     * @param b 将要发送的 集群内 leader 选举的 消息
     */
    public void toSend(Long sid, ByteBuffer b) {
        /*
         * If sending message to myself, then simply enqueue it (loopback).
         */
        // 简单的向自己发送查找 leader 的消息
        if (self.getId() == sid) {
             b.position(0);
             addToRecvQueue(new Message(b.duplicate(), sid));
            /*
             * Otherwise send to the corresponding thread to send.
             */
        } else { // 若 sid 不是自己的 myid, 也就是向集群中的其他机器发送 LOOKING 信息时进行下面操作
             /*
              * Start a new connection if doesn't have one already.
              */
             if (!queueSendMap.containsKey(sid)) { // 对应的 消息发送队列里面好没有对应的 queue, 则进行初始化
                 ArrayBlockingQueue<ByteBuffer> bq = new ArrayBlockingQueue<ByteBuffer>(SEND_CAPACITY);
                 queueSendMap.put(sid, bq);
                 addToSendQueue(bq, b);

             } else { // 根据 myid 来获取 对应的消息发送的 queue
                 ArrayBlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                 if(bq != null){
                     addToSendQueue(bq, b); // 将 消息加入到队列里面
                 } else {
                     LOG.error("No queue for server " + sid);
                 }
             }
             connectOne(sid); // 根据 sid 进行连接 QuorumPeer 节点
                
        }
    }
    
    /**
     * Try to establish a connection to server with id sid.
     * 
     *  @param sid  server id
     */
    // 与 对应的 sid 建立 socket 连接
    synchronized void connectOne(long sid){
        if (senderWorkerMap.get(sid) == null){                                 // 判断自己是否已经建立了 与 sid 的消息发送及接收
            InetSocketAddress electionAddr;
            if (self.quorumPeers.containsKey(sid)) {                            // 根据 sid 来获取 QuorumServer 的信息
                electionAddr = self.quorumPeers.get(sid).electionAddr;        // 获取对应的 leader 选举的地址
            } else {
                LOG.warn("Invalid server id: " + sid);
                return;
            }
            try {

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Opening channel to server " + sid);
                }
                Socket sock = new Socket();
                setSockOpts(sock);                                               // 进行 socket 的 Opts 的设置
                sock.connect(self.getView().get(sid).electionAddr, cnxTO);   // cnxTO 是进行连接的超时时间 (让人郁闷的是, 上面不是已经获取 electionAddr 了, 为什么还要从 map 里面重新获取一遍)
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Connected to server " + sid);
                }
                initiateConnection(sock, sid);
            } catch (UnresolvedAddressException e) {
                // Sun doesn't include the address that causes this
                // exception to be thrown, also UAE cannot be wrapped cleanly
                // so we log the exception in order to capture this critical
                // detail.
                LOG.warn("Cannot open channel to " + sid
                        + " at election address " + electionAddr, e);
                throw e;
            } catch (IOException e) {
                LOG.warn("Cannot open channel to " + sid
                        + " at election address " + electionAddr,
                        e);
            }
        } else {
            LOG.debug("There is a connection already for server " + sid);
        }
    }
    
    
    /**
     * Try to establish a connection with each server if one
     * doesn't exist.
     */
    // 遍历 queueSendMap 的keys (keys就是 myid), 来进行两两连接
    public void connectAll(){
        long sid;
        for(Enumeration<Long> en = queueSendMap.keys();
            en.hasMoreElements();){
            sid = en.nextElement();
            connectOne(sid);
        }      
    }
    

    /**
     * Check if all queues are empty, indicating that all messages have been delivered.
     */
    // 监测集群之间的消息是否都已经发送完了
    boolean haveDelivered() {
        for (ArrayBlockingQueue<ByteBuffer> queue : queueSendMap.values()) {
            LOG.debug("Queue size: " + queue.size());
            if (queue.size() == 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * Flag that it is time to wrap up all activities and interrupt the listener.
     */
    // 关闭集群中的各个连接
    public void halt() {
        shutdown = true;
        LOG.debug("Halting listener");
        listener.halt(); // 关闭这台 QuorumPeer 对应的, 用于监听集群之间消息的 socket
        
        softHalt();       // 关闭 集群之间两两连接建立的 (SenderWork, ReceiverWork)
    }
   
    /**
     * A soft halt simply finishes workers.
     */
    // 关闭所有消息发送/接收的线程
    public void softHalt() {
        for (SendWorker sw : senderWorkerMap.values()) {
            LOG.debug("Halting sender: " + sw);
            sw.finish(); // SendWorker 关闭的同时也会进行, receiverWorker 的关闭
        }
    }

    /**
     * Helper method to set socket options.
     * 
     * @param sock
     *            Reference to socket
     */
    private void setSockOpts(Socket sock) throws SocketException {
        sock.setTcpNoDelay(true);
        sock.setSoTimeout(self.tickTime * self.syncLimit); // 设置每次 有 QuorumPeer 请过过来, 但是又没有发消息过来的超时时间, 最后会爆出 EOFException 的异常, 重而进行退出
    }

    /**
     * Helper method to close a socket.
     * 
     * @param sock
     *            Reference to socket
     */
    private void closeSocket(Socket sock) {
        try {
            sock.close();
        } catch (IOException ie) {
            LOG.error("Exception while closing", ie);
        }
    }

    /**
     * Return number of worker threads
     */
    public long getThreadCount() {
        return threadCnt.get();
    }
    /**
     * Return reference to QuorumPeer
     */
    public QuorumPeer getQuorumPeer() {
        return self;
    }

    /**
     * Thread to listen on some port
     */
    public class Listener extends Thread {

        volatile ServerSocket ss = null;

        /**
         * Sleeps on accept().
         */
        @Override
        public void run() {
            int numRetries = 0;
            InetSocketAddress addr;
            while((!shutdown) && (numRetries < 3)){     // 有个疑惑 若真的出现 numRetries >= 3 从而退出了, 怎么办
                try {
                    ss = new ServerSocket();
                    ss.setReuseAddress(true);
                    if (self.getQuorumListenOnAllIPs()) { // 这里的默认值 quorumListenOnAllIPs 是 false
                        int port = self.quorumPeers.get(self.getId()).electionAddr.getPort();
                        addr = new InetSocketAddress(port);
                    } else {
                        addr = self.quorumPeers.get(self.getId()).electionAddr;
                    }
                    LOG.info("My election bind port: " + addr.toString());
                    setName(self.quorumPeers.get(self.getId()).electionAddr
                            .toString());
                    ss.bind(addr);
                    while (!shutdown) {
                        Socket client = ss.accept();  // 这里会阻塞, 直到有请求到达
                        setSockOpts(client);          // 设置 socket 的连接属性
                        LOG.info("Received connection request " + client.getRemoteSocketAddress());
                        receiveConnection(client);
                        numRetries = 0;
                    }
                } catch (IOException e) {
                    LOG.error("Exception while listening", e);
                    numRetries++;
                    try {
                        ss.close();
                        Thread.sleep(1000);
                    } catch (IOException ie) {
                        LOG.error("Error closing server socket", ie);
                    } catch (InterruptedException ie) {
                        LOG.error("Interrupted while sleeping. " +
                                  "Ignoring exception", ie);
                    }
                }
            }
            LOG.info("Leaving listener");
            if (!shutdown) {
                LOG.error("As I'm leaving the listener thread, "
                        + "I won't be able to participate in leader "
                        + "election any longer: "
                        + self.quorumPeers.get(self.getId()).electionAddr);
            }
        }
        
        /**
         * Halts this listener thread.
         */
        void halt(){
            try{
                LOG.debug("Trying to close listener: " + ss);
                if(ss != null) {
                    LOG.debug("Closing listener: " + self.getId());
                    ss.close();
                }
            } catch (IOException e){
                LOG.warn("Exception when shutting down listener: " + e);
            }
        }
    }

    /**
     * Thread to send messages. Instance waits on a queue, and send a message as
     * soon as there is one available. If connection breaks, then opens a new
     * one.
     */
    class SendWorker extends Thread {
        Long sid;
        Socket sock;
        RecvWorker recvWorker;
        volatile boolean running = true;
        DataOutputStream dout;

        /**
         * An instance of this thread receives messages to send
         * through a queue and sends them to the server sid.
         * 
         * @param sock
         *            Socket to remote peer
         * @param sid
         *            Server identifier of remote peer
         */
        SendWorker(Socket sock, Long sid) {
            super("SendWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            recvWorker = null;
            try {
                dout = new DataOutputStream(sock.getOutputStream());
            } catch (IOException e) {
                LOG.error("Unable to access socket output stream", e);
                closeSocket(sock);
                running = false;
            }
            LOG.debug("Address of remote peer: " + this.sid);
        }

        synchronized void setRecv(RecvWorker recvWorker) {
            this.recvWorker = recvWorker;
        }

        /**
         * Returns RecvWorker that pairs up with this SendWorker.
         * 
         * @return RecvWorker 
         */
        synchronized RecvWorker getRecvWorker(){
            return recvWorker;
        }
                
        synchronized boolean finish() {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Calling finish for " + sid);
            }
            
            if(!running){
                /*
                 * Avoids running finish() twice. 
                 */
                return running;
            }
            
            running = false;
            closeSocket(sock);
            // channel = null;

            this.interrupt();
            if (recvWorker != null) {
                recvWorker.finish();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Removing entry from senderWorkerMap sid=" + sid);
            }
            senderWorkerMap.remove(sid, this);
            threadCnt.decrementAndGet();
            return running;
        }
        
        synchronized void send(ByteBuffer b) throws IOException { // 进行消息的发送
            byte[] msgBytes = new byte[b.capacity()];
            try {
                b.position(0);
                b.get(msgBytes);
            } catch (BufferUnderflowException be) {
                LOG.error("BufferUnderflowException ", be);
                return;
            }
            dout.writeInt(b.capacity());                // 发送这个消息的长度
            dout.write(b.array());                      // 发送这个消息
            dout.flush();                               // 将消息刷到 远端
        }

        @Override
        public void run() {
            threadCnt.incrementAndGet();
            try {                                                           // 消息队列若为空, 则进行发送最后一个消息
                /**
                 * If there is nothing in the queue to send, then we
                 * send the lastMessage to ensure that the last message
                 * was received by the peer. The message could be dropped
                 * in case self or the peer shutdown their connection
                 * (and exit the thread) prior to reading/processing
                 * the last message. Duplicate messages are handled correctly
                 * by the peer.
                 *
                 * If the send queue is non-empty, then we have a recent
                 * message than that stored in lastMessage. To avoid sending
                 * stale message, we should send the message in the send queue.
                 */
                ArrayBlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                if (bq == null || isSendQueueEmpty(bq)) { // 检查 消息发送的队列是否为 空
                   ByteBuffer b = lastMessageSent.get(sid);
                   if (b != null) {
                       LOG.debug("Attempting to send lastMessage to sid=" + sid);
                       send(b);
                   }
                }
            } catch (IOException e) {
                LOG.error("Failed to send last message. Shutting down thread.", e);
                this.finish();
            }
            
            try {
                while (running && !shutdown && sock != null) {

                    ByteBuffer b = null;
                    try {
                        ArrayBlockingQueue<ByteBuffer> bq = queueSendMap
                                .get(sid);
                        if (bq != null) {
                            b = pollSendQueue(bq, 1000, TimeUnit.MILLISECONDS);         // 从队列里面 poll 出来消息
                        } else {
                            LOG.error("No queue of incoming messages for " +
                                      "server " + sid);
                            break;
                        }

                        if(b != null){
                            lastMessageSent.put(sid, b);                              // 将消息放入 对应 map
                            send(b);                                                     // 进行消息的发送
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for message on queue",
                                e);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception when using channel: for id " + sid + " my id = " + 
                        self.getId() + " error = " + e);
            }
            this.finish();
            LOG.warn("Send worker leaving thread");
        }
    }

    /**
     * Thread to receive messages. Instance waits on a socket read. If the
     * channel breaks, then removes itself from the pool of receivers.
     */
    // 集群中一个连接对应一个 RecvWorker 专门进行消息的接收
    class RecvWorker extends Thread {
        Long sid;
        Socket sock;
        volatile boolean running = true;
        DataInputStream din;
        final SendWorker sw;

        RecvWorker(Socket sock, Long sid, SendWorker sw) {
            super("RecvWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            this.sw = sw;
            try {
                din = new DataInputStream(sock.getInputStream());
                // OK to wait until socket disconnects while reading.
                sock.setSoTimeout(0);
            } catch (IOException e) {
                LOG.error("Error while accessing socket for " + sid, e);
                closeSocket(sock);
                running = false;
            }
        }
        
        /**
         * Shuts down this worker
         * 
         * @return boolean  Value of variable running
         */
        synchronized boolean finish() {
            if(!running){
                /*
                 * Avoids running finish() twice. 
                 */
                return running;
            }
            running = false;            

            this.interrupt();
            threadCnt.decrementAndGet();
            return running;
        }

        @Override
        public void run() {                                                    // 这个 loop 就是从 socket 里面进行数据的读取, 然后加入到队列, 简单的惨目忍睹啊
            threadCnt.incrementAndGet();
            try {
                while (running && !shutdown && sock != null) {
                    /**
                     * Reads the first int to determine the length of the
                     * message
                     */
                    int length = din.readInt();                                 // 读取这条消息的长度
                    if (length <= 0 || length > PACKETMAXSIZE) {
                        throw new IOException(
                                "Received packet with invalid packet: "
                                        + length);
                    }
                    /**
                     * Allocates a new ByteBuffer to receive the message
                     */
                    byte[] msgArray = new byte[length];
                    din.readFully(msgArray, 0, length);                         // 读取整个消息
                    ByteBuffer message = ByteBuffer.wrap(msgArray);             // 使用 ByteBuffer 包裹一下 byte 数组
                    addToRecvQueue(new Message(message.duplicate(), sid));     // 将消息加入队列 (注意, 前方高能, 所有 ReceiveWork 公用一个  Queue)
                }
            } catch (Exception e) {
                LOG.warn("Connection broken for id " + sid + ", my id = " + 
                        self.getId() + ", error = " , e);
            } finally {
                LOG.warn("Interrupting SendWorker");
                sw.finish();                                                    // 关闭 SenderWork
                if (sock != null) {                                            // 关闭自己的 socket
                    closeSocket(sock);
                }
            }
        }
    }

    /**
     * Inserts an element in the specified queue. If the Queue is full, this
     * method removes an element from the head of the Queue and then inserts
     * the element at the tail. It can happen that the an element is removed
     * by another thread in {@link SendWorker#processMessage() processMessage}
     * method before this method attempts to remove an element from the queue.
     * This will cause {@link ArrayBlockingQueue#remove() remove} to throw an
     * exception, which is safe to ignore.
     *
     * Unlike {@link #addToRecvQueue(Message) addToRecvQueue} this method does
     * not need to be synchronized since there is only one thread that inserts
     * an element in the queue and another thread that reads from the queue.
     *
     * @param queue
     *          Reference to the Queue
     * @param buffer
     *          Reference to the buffer to be inserted in the queue
     */
    private void addToSendQueue(ArrayBlockingQueue<ByteBuffer> queue,
          ByteBuffer buffer) {
        if (queue.remainingCapacity() == 0) { // 为什么 这里要进行删除 (从头部删除, 尾部插入)
            try { //
                queue.remove(); // 看了一下 这里的 remove 会调用 ArrayBlockingQueue 里面的 poll 方法, 从而进行 数据的删除
            } catch (NoSuchElementException ne) {
                // element could be removed by poll()
                LOG.debug("Trying to remove from an empty " +
                        "Queue. Ignoring exception " + ne);
            }
        }
        try {
            queue.add(buffer);
        } catch (IllegalStateException ie) {
            // This should never happen
            LOG.error("Unable to insert an element in the queue " + ie);
        }
    }

    /**
     * Returns true if queue is empty.
     * @param queue
     *          Reference to the queue
     * @return
     *      true if the specified queue is empty
     */
    private boolean isSendQueueEmpty(ArrayBlockingQueue<ByteBuffer> queue) {
        return queue.isEmpty();
    }

    /**
     * Retrieves and removes buffer at the head of this queue,
     * waiting up to the specified wait time if necessary for an element to
     * become available.
     *
     * {@link ArrayBlockingQueue#poll(long, java.util.concurrent.TimeUnit)}
     */
    private ByteBuffer pollSendQueue(ArrayBlockingQueue<ByteBuffer> queue,
          long timeout, TimeUnit unit) throws InterruptedException {
       return queue.poll(timeout, unit);
    }

    /**
     * Inserts an element in the {@link #recvQueue}. If the Queue is full, this
     * methods removes an element from the head of the Queue and then inserts
     * the element at the tail of the queue.
     *
     * This method is synchronized to achieve fairness between two threads that
     * are trying to insert an element in the queue. Each thread checks if the
     * queue is full, then removes the element at the head of the queue, and
     * then inserts an element at the tail. This three-step process is done to
     * prevent a thread from blocking while inserting an element in the queue.
     * If we do not synchronize the call to this method, then a thread can grab
     * a slot in the queue created by the second thread. This can cause the call
     * to insert by the second thread to fail.
     * Note that synchronizing this method does not block another thread
     * from polling the queue since that synchronization is provided by the
     * queue itself.
     *
     * @param msg
     *          Reference to the message to be inserted in the queue
     */
    public void addToRecvQueue(Message msg) {
        synchronized(recvQLock) {
            if (recvQueue.remainingCapacity() == 0) {       // 纠结 在queue满时会删除最前面一个元素, 在tail位置加上一个节点
                try {
                    recvQueue.remove();
                } catch (NoSuchElementException ne) {
                    // element could be removed by poll()
                     LOG.debug("Trying to remove from an empty " +
                         "recvQueue. Ignoring exception " + ne);
                }
            }
            try {
                recvQueue.add(msg);                         // ArrayBlockingQueue 在尾部加入节点元素
            } catch (IllegalStateException ie) {
                // This should never happen
                LOG.error("Unable to insert element in the recvQueue " + ie);
            }
        }
    }

    /**
     * Retrieves and removes a message at the head of this queue,
     * waiting up to the specified wait time if necessary for an element to
     * become available.
     *
     * {@link ArrayBlockingQueue#poll(long, java.util.concurrent.TimeUnit)}
     */
    public Message pollRecvQueue(long timeout, TimeUnit unit)
       throws InterruptedException {
       return recvQueue.poll(timeout, unit);
    }
}

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
package org.apache.zookeeper.server.persistence;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the TxnLog interface. It provides api's
 * to access the txnlogs and add entries to it.
 * <p>
 * The format of a Transactional log is as follows:
 * <blockquote><pre>
 * LogFile:
 *     FileHeader TxnList ZeroPad
 * 
 * FileHeader: {
 *     magic 4bytes (ZKLG)
 *     version 4bytes
 *     dbid 8bytes
 *   }
 * 
 * TxnList:
 *     Txn || Txn TxnList
 *     
 * Txn:
 *     checksum Txnlen TxnHeader Record 0x42
 * 
 * checksum: 8bytes Adler32 is currently used
 *   calculated across payload -- Txnlen, TxnHeader, Record and 0x42
 * 
 * Txnlen:
 *     len 4bytes
 * 
 * TxnHeader: {
 *     sessionid 8bytes
 *     cxid 4bytes
 *     zxid 8bytes
 *     time 8bytes
 *     type 4bytes
 *   }
 *     
 * Record:
 *     See Jute definition file for details on the various record types
 *      
 * ZeroPad:
 *     0 padded to EOF (filled during preallocation stage)
 * </pre></blockquote> 
 */
public class FileTxnLog implements TxnLog {
    private static final Logger LOG;

    public static long preAllocSize =  65536 * 1024;                                   //  预分配的 TxnLog 文件大小为 64M

    public final static int TXNLOG_MAGIC =
        ByteBuffer.wrap("ZKLG".getBytes()).getInt();

    public final static int VERSION = 2;

    /** Maximum time we allow for elapsed fsync before WARNing */
    private final static long fsyncWarningThresholdMS;                          // 持久化时间过长的报警阀值, 默认值 1秒

    static {
        LOG = LoggerFactory.getLogger(FileTxnLog.class);

        String size = System.getProperty("zookeeper.preAllocSize");             // 解析出 TxnLog 预分配文件的大小
        if (size != null) {
            try {
                preAllocSize = Long.parseLong(size) * 1024;
            } catch (NumberFormatException e) {
                LOG.warn(size + " is not a valid value for preAllocSize");
            }
        }
        fsyncWarningThresholdMS = Long.getLong("fsync.warningthresholdms", 1000);
    }

    long lastZxidSeen;
    public volatile BufferedOutputStream logStream = null;
    volatile OutputArchive oa;
    volatile FileOutputStream fos = null;

    File logDir;
    private final boolean forceSync = !System.getProperty("zookeeper.forceSync", "yes").equals("no");;
    long dbId;
    private LinkedList<FileOutputStream> streamsToFlush =
        new LinkedList<FileOutputStream>();
    long currentSize;                                                   // 记录当前文件 txnLogFile 的大小
    File logFileWrite = null;

    /**
     * constructor for FileTxnLog. Take the directory
     * where the txnlogs are stored
     * @param logDir the directory where the txnlogs are stored
     */
    public FileTxnLog(File logDir) {
        this.logDir = logDir;
    }

    /**
     * method to allow setting preallocate size
     * of log file to pad the file.
     * @param size the size to set to in bytes
     */
    public static void setPreallocSize(long size) {
        preAllocSize = size;
    }

    /**
     * creates a checksum alogrithm to be used
     * @return the checksum used for this txnlog
     */
    protected Checksum makeChecksumAlgorithm(){
        return new Adler32();
    }


    /**
     * rollover the current log file to a new one.
     * @throws IOException
     */
    // 滚动
    public synchronized void rollLog() throws IOException {
        if (logStream != null) {
            this.logStream.flush(); // 将数据直接
            this.logStream = null;  // 因为每次进行 写 Txn 数据到 日志里面都会判断 logStream != null, 所以通过这种方式来新建 TxnLog
            oa = null;
        }
    }

    /**
     * close all the open file handles
     * @throws IOException
     */
    public synchronized void close() throws IOException {
        if (logStream != null) {
            logStream.close();
        }
        for (FileOutputStream log : streamsToFlush) {
            log.close();
        }
    }
    
    /**
     * 追加事物信息到事物日志里面
     * append an entry to the transaction log
     * @param hdr the header of the transaction 事务头
     * @param txn the transaction part of the entry 事务体
     * returns true iff something appended, otw false 
     */
    public synchronized boolean append(TxnHeader hdr, Record txn) throws IOException
    {
        if (hdr != null) {
            if (hdr.getZxid() <= lastZxidSeen) {                                // 若接收到的 txn 的 zxid 小于已经处理的, 则进行 log 一下
                LOG.warn("Current zxid " + hdr.getZxid()
                        + " is <= " + lastZxidSeen + " for "
                        + hdr.getType());
            }
            if (logStream==null) {                                              // 每次进行 append txn 时,  通过判断 logStream == null 来决定是否生成新的 TxnLogFile
               if(LOG.isInfoEnabled()){
                    LOG.info("Creating new log file: log." +  
                            Long.toHexString(hdr.getZxid()));
               }
                                                                                // 构建 TxnLogFile, 这里的文件名中含有的 zxid 其实就是 ZKDataTree 处理的最新的 Txn
               logFileWrite = new File(logDir, ("log." + Long.toHexString(hdr.getZxid())));
               fos = new FileOutputStream(logFileWrite);
               logStream = new BufferedOutputStream(fos);
               oa = BinaryOutputArchive.getArchive(logStream);                  // 构建对应的数据流
               FileHeader fhdr = new FileHeader(TXNLOG_MAGIC,VERSION, dbId);    // 每个文件的文件头,
               fhdr.serialize(oa, "fileheader");                                // 将 FileHeader 序列化到流里面
               // Make sure that the magic number is written before padding.
               logStream.flush();
               currentSize = fos.getChannel().position();
               streamsToFlush.add(fos);                                         // 加入到 TxnFileStream 组里面
            }
            padFile(fos);                                                       // 将 TxnLog 文件扩充到指定文件的大小 preAllocSize
            byte[] buf = Util.marshallTxnEntry(hdr, txn);                       // 将 hdr, txn 序列化出 byte[]
            if (buf == null || buf.length == 0) {
                throw new IOException("Faulty serialization for header " +
                        "and txn");
            }
            Checksum crc = makeChecksumAlgorithm();                             // 获取计算数据流校验码的算法
            crc.update(buf, 0, buf.length);                                     // 计算数据( hdr, txn )完整性的校验码
            oa.writeLong(crc.getValue(), "txnEntryCRC");                        // 将( hdr, txn )对应的校验码写入数据流
            Util.writeTxnBytes(oa, buf);                                        // 将 hdr, txn 对应的字节数组 buf 写入数据流里面
            
            return true;
        }
        return false;
    }

    /**
     * pad the current file to increase its size
     * @param out the outputstream to be padded
     * @throws IOException
     */
    // 校验并且扩充文件的大小
    private void padFile(FileOutputStream out) throws IOException {                         // 扩充文件
        currentSize = Util.padLogFile(out, currentSize, preAllocSize);                      // 最终的大小是 currentSize(详情见 Util.padLogFile)
    }

    /**
     * Find the log file that starts at, or just before, the snapshot. Return
     * this and all subsequent logs. Results are ordered by zxid of file,
     * ascending order.
     * @param logDirList array of files
     * @param snapshotZxid return files at, or before this zxid
     * @return
     *
     */
    // 得到 大于 snapshotZxid 的所有 txn log 文件
    public static File[] getLogFiles(File[] logDirList,long snapshotZxid) {
        List<File> files = Util.sortDataDir(logDirList, "log", true);               // 将所有的 txn log 进行升续排序( true 代表升序)
        long logZxid = 0;
        // Find the log file that starts before or at the same time as the
        // zxid of the snapshot
        for (File f : files) {
            long fzxid = Util.getZxidFromName(f.getName(), "log");                  // 从 txn log 文件名获取 对应  zxid, 若前缀不是 log, 则直接返回 -1
            if (fzxid > snapshotZxid) {                                             // fzxid > snapshotZxid 说明, 对应的 txn log 还需要保留
                continue;
            }
            // the files
            // are sorted with zxid's
            if (fzxid > logZxid) {                                                  // 获取要删除的 txn log 文件中 最大的 zxid 的值
                logZxid = fzxid;
            }
        }
                                                                                    // 程序运行到这里, 其实logZxid 是小于 snapshotZxid 的最大的 zxid
        List<File> v=new ArrayList<File>(5);
        for (File f : files) {                                                      // 再次过滤 txn log, 通过 (fzxid < logZxid) 筛选出 要进行删除的 txn log 文件
            long fzxid = Util.getZxidFromName(f.getName(), "log");
            if (fzxid < logZxid) {                                                  // 将大于这个的所有 TxnLog 获取出来
                continue;
            }
            v.add(f);
        }
        return v.toArray(new File[0]);

    }

    /**
     * get the last zxid that was logged in the transaction logs
     * @return the last zxid logged in the transaction logs
     */
    // 基于 txn log file, 查找出 ZooKeeperServer
    public long getLastLoggedZxid() {
        File[] files = getLogFiles(logDir.listFiles(), 0);                           // 获取 大于 zxid = 0 的所有的 txn log 文件
        long maxLog=files.length>0?
                Util.getZxidFromName(files[files.length-1].getName(),"log"):-1;     // 获取最大一个文件的 zxid (PS: 这里有个注意点, 每个 txn log 文件名中的 zxid, 代表着这个 txn log 日志中存储的最小 zxid )

        // if a log file is more recent we must scan it to find
        // the highest zxid
        long zxid = maxLog;
        TxnIterator itr = null;
        try {
            FileTxnLog txn = new FileTxnLog(logDir);
            itr = txn.read(maxLog);                                                  // 在 FileTxnIterator 里面会构建一个降序排序的 txn log file
            while (true) {
                if(!itr.next())                                                     // 不断读取 txn log 里面的 txn 日志 (因为 lastzxid 的 txn log 信息是写在这个文件的最后一个)
                    break;
                TxnHeader hdr = itr.getHeader();
                zxid = hdr.getZxid();
            }
        } catch (IOException e) {
            LOG.warn("Unexpected exception", e);
        } finally {
            close(itr);
        }
        return zxid;
    }

    private void close(TxnIterator itr) {
        if (itr != null) {
            try {
                itr.close();
            } catch (IOException ioe) {
                LOG.warn("Error closing file iterator", ioe);
            }
        }
    }

    /**
     * commit the logs. make sure that evertyhing hits the
     * disk
     */
    // 将 TxnLog 的 buffer 数据 flush 到磁盘上 (一般是 Txn log 超过 1000 就会在 SyncRequestProcessor 中出发落磁盘)
    public synchronized void commit() throws IOException {
        if (logStream != null) {
            logStream.flush();
        }
        for (FileOutputStream log : streamsToFlush) {           // 将FileStream 里面的 Txn Log 文件落磁盘
            log.flush();
            if (forceSync) {
                long startSyncNS = System.nanoTime();

                log.getChannel().force(false);                   // 强行将数据刷到磁盘上 (when this method returns it is guaranteed that all changes made to the file since this channel was created 详情见注解部分)
                                                                  // 监控是否落磁盘时间超限制
                long syncElapsedMS = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startSyncNS);
                if (syncElapsedMS > fsyncWarningThresholdMS) {  // 判断刷数据到磁盘是否超过系统的阀值
                    LOG.warn("fsync-ing the write ahead log in "
                            + Thread.currentThread().getName()
                            + " took " + syncElapsedMS
                            + "ms which will adversely effect operation latency. "
                            + "See the ZooKeeper troubleshooting guide");
                }
            }
        }
        while (streamsToFlush.size() > 1) {                    // 将对应的 File Stream 都进行删除(都已经将数据刷到磁盘上了, 没必要保留)
            streamsToFlush.removeFirst().close();
        }
    }

    /**
     * start reading all the transactions from the given zxid
     * @param zxid the zxid to start reading transactions from
     * @return returns an iterator to iterate through the transaction
     * logs
     */
    public TxnIterator read(long zxid) throws IOException {
        return new FileTxnIterator(logDir, zxid);
    }

    /**
     * truncate the current transaction logs
     * @param zxid the zxid to truncate the logs to
     * @return true if successful false if not
     */
    public boolean truncate(long zxid) throws IOException {
        FileTxnIterator itr = null;
        try {
            itr = new FileTxnIterator(this.logDir, zxid);                   // 获取 大于 zxid 的 txnLog 文件
            PositionInputStream input = itr.inputStream;
            long pos = input.getPosition();                                  // 这一步非常重要, 因为 下次写txn log 就会从这个地方开始写
            // now, truncate at the current position
            RandomAccessFile raf = new RandomAccessFile(itr.logFile, "rw");
            raf.setLength(pos);
            raf.close();
            while (itr.goToNextLog()) {                                     // 将 cursor 定位到 ArrayList 的尾部
                if (!itr.logFile.delete()) {
                    LOG.warn("Unable to truncate {}", itr.logFile);
                }
            }
        } finally {
            close(itr);
        }
        return true;
    }

    /**
     * read the header of the transaction file
     * @param file the transaction file to read
     * @return header that was read fomr the file
     * @throws IOException
     */
    // 读取 txnLog 里面的 FileHeader
    private static FileHeader readHeader(File file) throws IOException {
        InputStream is =null;
        try {
            is = new BufferedInputStream(new FileInputStream(file));
            InputArchive ia=BinaryInputArchive.getArchive(is);
            FileHeader hdr = new FileHeader();
            hdr.deserialize(ia, "fileheader");
            return hdr;
         } finally {
             try {
                 if (is != null) is.close();
             } catch (IOException e) {
                 LOG.warn("Ignoring exception during close", e);
             }
         }
    }

    /**
     * the dbid of this transaction database
     * @return the dbid of this database
     */
    // 获取文件的 dbId(这里是默认值 -1)
    public long getDbId() throws IOException {
        FileTxnIterator itr = new FileTxnIterator(logDir, 0);
        FileHeader fh=readHeader(itr.logFile);
        itr.close();
        if(fh==null)
            throw new IOException("Unsupported Format.");
        return fh.getDbid();
    }

    /**
     * the forceSync value. true if forceSync is enabled, false otherwise.
     * @return the forceSync value
     */
    public boolean isForceSync() {
        return forceSync;
    }

    /**
     * a class that keeps track of the position 
     * in the input stream. The position points to offset
     * that has been consumed by the applications. It can 
     * wrap buffered input streams to provide the right offset 
     * for the application.
     */
    static class PositionInputStream extends FilterInputStream {
        long position;
        protected PositionInputStream(InputStream in) {
            super(in);
            position = 0;
        }
        
        @Override
        public int read() throws IOException {
            int rc = super.read();
            if (rc > -1) {
                position++;
            }
            return rc;
        }

        public int read(byte[] b) throws IOException {
            int rc = super.read(b);
            if (rc > 0) {
                position += rc;
            }
            return rc;            
        }
        
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int rc = super.read(b, off, len);
            if (rc > 0) {
                position += rc;
            }
            return rc;
        }
        
        @Override
        public long skip(long n) throws IOException {
            long rc = super.skip(n);
            if (rc > 0) {
                position += rc;
            }
            return rc;
        }
        public long getPosition() {
            return position;
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void mark(int readLimit) {
            throw new UnsupportedOperationException("mark");
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException("reset");
        }
    }
    
    /**
     * this class implements the txnlog iterator interface
     * which is used for reading the transaction logs
     */
    public static class FileTxnIterator implements TxnLog.TxnIterator {
        File logDir;
        long zxid;
        TxnHeader hdr;
        Record record;
        File logFile;
        InputArchive ia;
        static final String CRC_ERROR="CRC check failed";
       
        PositionInputStream inputStream=null;
        //stored files is the list of files greater than
        //the zxid we are looking for.
        private ArrayList<File> storedFiles;

        /**
         * create an iterator over a transaction database directory
         * @param logDir the transaction database directory
         * @param zxid the zxid to start reading from
         * @throws IOException
         */
        public FileTxnIterator(File logDir, long zxid) throws IOException {
          this.logDir = logDir;
          this.zxid = zxid;
          init();
        }

        /**
         * initialize to the zxid specified
         * this is inclusive of the zxid
         * @throws IOException
         */
        void init() throws IOException {
            storedFiles = new ArrayList<File>();                        // 获取 zxid 大于 0 的所有 txn 日志, 并且降序排序
            List<File> files = Util.sortDataDir(FileTxnLog.getLogFiles(logDir.listFiles(), 0), "log", false);
            for (File f: files) {
                if (Util.getZxidFromName(f.getName(), "log") >= zxid) {
                    storedFiles.add(f);
                }
                // add the last logfile that is less than the zxid
                else if (Util.getZxidFromName(f.getName(), "log") < zxid) {
                    storedFiles.add(f);
                    break;
                }
            }
            goToNextLog();                                              // 将刚才获取的  files取出, 并将 zxid 最小的初始化 PositionInputStream
            if (!next())                                                // 每次调用 next 都会将 下个事务处理信息读取出来
                return;
            while (hdr.getZxid() < zxid) {                            // 这里就是在 通过 next() 方法, 不断轮训, 定位到 zxid 的位置
                if (!next())
                    return;
            }
        }

        /**
         * go to the next logfile
         * @return true if there is one and false if there is no
         * new file to be read
         * @throws IOException
         */
        // 迭代到下个 Txn Log 文件
        private boolean goToNextLog() throws IOException {
            if (storedFiles.size() > 0) {
                this.logFile = storedFiles.remove(storedFiles.size()-1);
                ia = createInputArchive(this.logFile);
                return true;
            }
            return false;
        }

        /**
         * read the header from the inputarchive
         * @param ia the inputarchive to be read from
         * @param is the inputstream
         * @throws IOException
         */
        // 取出 Fileheader 并进行相应的校验
        protected void inStreamCreated(InputArchive ia, InputStream is)
            throws IOException{
            FileHeader header= new FileHeader();
            header.deserialize(ia, "fileheader");
            if (header.getMagic() != FileTxnLog.TXNLOG_MAGIC) {
                throw new IOException("Transaction log: " + this.logFile + " has invalid magic number " 
                        + header.getMagic()
                        + " != " + FileTxnLog.TXNLOG_MAGIC);
            }
        }

        /**
         * Invoked to indicate that the input stream has been created.
         * @param ia input archive
         * @param is file input stream associated with the input archive.
         * @throws IOException
         **/
        // 创建 logFile 对应的数据流
        protected InputArchive createInputArchive(File logFile) throws IOException {
            if(inputStream==null){
                inputStream= new PositionInputStream(new BufferedInputStream(new FileInputStream(logFile)));
                LOG.debug("Created new input stream " + logFile);
                ia  = BinaryInputArchive.getArchive(inputStream);
                inStreamCreated(ia,inputStream);
                LOG.debug("Created new input archive " + logFile);
            }
            return ia;
        }

        /**
         * create a checksum algorithm
         * @return the checksum algorithm
         */
        protected Checksum makeChecksumAlgorithm(){
            return new Adler32();
        }

        /**
         * the iterator that moves to the next transaction
         * @return true if there is more transactions to be read
         * false if not.
         */
        // 从数据流中不断读取 Txn log 记录, 一次读取一条 (若这里看不懂, 那就看一下数据写入的流程)
        public boolean next() throws IOException {
            if (ia == null) {
                return false;
            }
            try {
                long crcValue = ia.readLong("crcvalue");
                byte[] bytes = Util.readTxnBytes(ia);
                // Since we preallocate, we define EOF to be an
                if (bytes == null || bytes.length==0) {
                    throw new EOFException("Failed to read " + logFile);
                }
                // EOF or corrupted record
                // validate CRC
                Checksum crc = makeChecksumAlgorithm();
                crc.update(bytes, 0, bytes.length);
                if (crcValue != crc.getValue())
                    throw new IOException(CRC_ERROR);
                if (bytes == null || bytes.length == 0)
                    return false;
                hdr = new TxnHeader();
                record = SerializeUtils.deserializeTxn(bytes, hdr);
            } catch (EOFException e) {
                LOG.debug("EOF excepton " + e);
                inputStream.close();
                inputStream = null;
                ia = null;
                hdr = null;
                // this means that the file has ended
                // we should go to the next file
                if (!goToNextLog()) {
                    return false;
                }
                // if we went to the next log file, we should call next() again
                return next();
            } catch (IOException e) {
                inputStream.close();
                throw e;
            }
            return true;
        }

        /**
         * reutrn the current header
         * @return the current header that
         * is read
         */
        public TxnHeader getHeader() {
            return hdr;
        }

        /**
         * return the current transaction
         * @return the current transaction
         * that is read
         */
        public Record getTxn() {
            return record;
        }

        /**
         * close the iterator
         * and release the resources.
         */
        public void close() throws IOException {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

}

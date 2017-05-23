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

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * A collection of utility methods for dealing with file name parsing, 
 * low level I/O file operations and marshalling/unmarshalling.
 */
public class Util {
    private static final Logger LOG = LoggerFactory.getLogger(Util.class);
    private static final String SNAP_DIR="snapDir";
    private static final String LOG_DIR="logDir";
    private static final String DB_FORMAT_CONV="dbFormatConversion";
    private static final ByteBuffer fill = ByteBuffer.allocateDirect(1);
    
    public static String makeURIString(String dataDir, String dataLogDir, 
            String convPolicy){
        String uri="file:"+SNAP_DIR+"="+dataDir+";"+LOG_DIR+"="+dataLogDir;
        if(convPolicy!=null)
            uri+=";"+DB_FORMAT_CONV+"="+convPolicy;
        return uri.replace('\\', '/');
    }
    /**
     * Given two directory files the method returns a well-formed 
     * logfile provider URI. This method is for backward compatibility with the
     * existing code that only supports logfile persistence and expects these two
     * parameters passed either on the command-line or in the configuration file.
     * 
     * @param dataDir snapshot directory
     * @param dataLogDir transaction log directory
     * @return logfile provider URI
     */
    public static URI makeFileLoggerURL(File dataDir, File dataLogDir){
        return URI.create(makeURIString(dataDir.getPath(),dataLogDir.getPath(),null));
    }
    
    public static URI makeFileLoggerURL(File dataDir, File dataLogDir,String convPolicy){
        return URI.create(makeURIString(dataDir.getPath(),dataLogDir.getPath(),convPolicy));
    }

    /**
     * Creates a valid transaction log file name. 
     * 
     * @param zxid used as a file name suffix (extention)
     * @return file name
     */
    public static String makeLogName(long zxid) {
        return "log." + Long.toHexString(zxid);
    }

    /**
     * Creates a snapshot file name.
     * 
     * @param zxid used as a suffix
     * @return file name
     */
    public static String makeSnapshotName(long zxid) {
        return "snapshot." + Long.toHexString(zxid);
    }
    
    /**
     * Extracts snapshot directory property value from the container.
     * 
     * @param props properties container
     * @return file representing the snapshot directory
     */
    public static File getSnapDir(Properties props){
        return new File(props.getProperty(SNAP_DIR));
    }

    /**
     * Extracts transaction log directory property value from the container.
     * 
     * @param props properties container
     * @return file representing the txn log directory
     */
    public static File getLogDir(Properties props){
        return new File(props.getProperty(LOG_DIR));
    }
    
    /**
     * Extracts the value of the dbFormatConversion attribute.
     * 
     * @param props properties container
     * @return value of the dbFormatConversion attribute
     */
    public static String getFormatConversionPolicy(Properties props){
        return props.getProperty(DB_FORMAT_CONV);
    }
   
    /**
     * Extracts zxid from the file name. The file name should have been created
     *
     * @param name the file name to parse
     * @param prefix the file name prefix (snapshot or log)
     * @return zxid
     */
    public static long getZxidFromName(String name, String prefix) {
        long zxid = -1;
        String nameParts[] = name.split("\\.");
        if (nameParts.length == 2 && nameParts[0].equals(prefix)) {         // 这里通过 prefix 来区分 snapshot 还是 txnlog
            try {
                zxid = Long.parseLong(nameParts[1], 16);
            } catch (NumberFormatException e) {
            }
        }
        return zxid;
    }

    /**
     * Verifies that the file is a valid snapshot. Snapshot may be invalid if 
     * it's incomplete as in a situation when the server dies while in the process
     * of storing a snapshot. Any file that is not a snapshot is also 
     * an invalid snapshot. 
     * 
     * @param f file to verify
     * @return true if the snapshot is valid
     * @throws IOException
     */
    /**
     * 校验 snapshot 文件是否有效
     */
    public static boolean isValidSnapshot(File f) throws IOException {
        if (f==null || Util.getZxidFromName(f.getName(), "snapshot") == -1)
            return false;

        // Check for a valid snapshot
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        try {
            // including the header and the last / bytes
            // the snapshot should be atleast 10 bytes
            // 文件的大小小于 10, 则说明文件不正确 (文件有对应的文件头)
            if (raf.length() < 10) {                      // 1.文件过小, 直接返回 false
                return false;
            }
                                                          // 2.磁盘seek(定位) 到最后 5个字节的地方
            raf.seek(raf.length() - 5);
            byte bytes[] = new byte[5];
            int readlen = 0;
            int l;
                                                          // 3.读取文件的最后 5 个字节
            while(readlen < 5 &&
                  (l = raf.read(bytes, readlen, bytes.length - readlen)) >= 0) {
                readlen += l;
            }
            if (readlen != bytes.length) {                 // 4.是否读取出正确的字节数
                LOG.info("Invalid snapshot " + f + " too short, len = " + readlen);
                return false;
            }
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            int len = bb.getInt();
            byte b = bb.get();
            // 校验这个 5 个字节是否是 1 和 '/'
            if (len != 1 || b != '/') {                     // 5.判断是否获取了 5个字节, 并且是否取出的是 "/"
                LOG.info("Invalid snapshot " + f + " len = " + len + " byte = " + (b & 0xff));
                return false;
            }
        } finally {
            raf.close();
        }

        return true;
    }

    /**
     * Grows the file to the specified number of bytes. This only happenes if 
     * the current file position is sufficiently close (less than 4K) to end of 
     * file. 
     * 
     * @param f output stream to pad
     * @param currentSize application keeps track of the cuurent file size
     * @param preAllocSize how many bytes to pad
     * @return the new file size. It can be the same as currentSize if no
     * padding was done.
     * @throws IOException
     */
    public static long padLogFile(FileOutputStream f,long currentSize, long preAllocSize) throws IOException{
        long position = f.getChannel().position();                      // 1. 获取数据流现在的位置
        LOG.info("padLogFile_position:" + position + ", currentSize:" + currentSize);
        if (position + 4096 >= currentSize) {                           // 2. 这里经过第一次 padLogFile 调用后, currentSize 就变成了 64M了, 但是当position 够大时(position + 4096 > 64M), 则就进行再次扩充 64M
            currentSize = currentSize + preAllocSize;                   // 3. 扩充 64M, 每次扩充 64M
            fill.position(0);                                           // 4. The fill from which bytes are to be transferred to the f (FileOutputStream)
            long remaining = fill.remaining();
            LOG.info("fill.remaining():" + fill.remaining());           // 5. 将文件扩充 (currentSize-fill.remaining()) 大小
            LOG.info("padLogFile_position:" + position + ", currentSize:" + currentSize + ", remaining:" + remaining);
            f.getChannel().write(fill, currentSize-fill.remaining());   // 6. 这一步就是扩充 f 的大小, 直接到  (currentSize-fill.remaining()), 并且 这些扩充空间的内容不会指定
        }
        return currentSize;
    }

    /**
     * Reads a transaction entry from the input archive.
     * @param ia archive to read from
     * @return null if the entry is corrupted or EOF has been reached; a buffer
     * (possible empty) containing serialized transaction record.
     * @throws IOException
     */
    public static byte[] readTxnBytes(InputArchive ia) throws IOException {
        try{
            byte[] bytes = ia.readBuffer("txtEntry");
            // Since we preallocate, we define EOF to be an
            // empty transaction
            if (bytes.length == 0)
                return bytes;
            if (ia.readByte("EOF") != 'B') {
                LOG.error("Last transaction was partial.");
                return null;
            }
            return bytes;
        }catch(EOFException e){}
        return null;
    }
    

    /**
     * Serializes transaction header and transaction data into a byte buffer.
     *  
     * @param hdr transaction header
     * @param txn transaction data
     * @return serialized transaction record
     * @throws IOException
     */
    // 整理编排 请求头与请求体, 先将对应的数据进行序列化, 然后返回对应的字节数组
    public static byte[] marshallTxnEntry(TxnHeader hdr, Record txn) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputArchive boa = BinaryOutputArchive.getArchive(baos);

        hdr.serialize(boa, "hdr");
        if (txn != null) {
            txn.serialize(boa, "txn");
        }
        return baos.toByteArray();
    }

    /**
     * Write the serialized transaction record to the output archive.
     *  
     * @param oa output archive
     * @param bytes serialized trasnaction record
     * @throws IOException
     */
    public static void writeTxnBytes(OutputArchive oa, byte[] bytes)
            throws IOException {
        oa.writeBuffer(bytes, "txnEntry"); // 将数据流写入 OutputStream
        oa.writeByte((byte) 0x42, "EOR"); // 'B'
    }
    
    
    /**
     * Compare file file names of form "prefix.version". Sort order result
     * returned in order of version.
     */
    // 构件出比较 文件名来排序 snapshotFile 的 comparator
    private static class DataDirFileComparator
        implements Comparator<File>, Serializable
    {
        private static final long serialVersionUID = -2648639884525140318L;

        private String prefix;
        private boolean ascending;
        public DataDirFileComparator(String prefix, boolean ascending) {
            this.prefix = prefix;
            this.ascending = ascending;
        }
        // 根据 zxid 来进行排序 事务日志文件
        public int compare(File o1, File o2) {
            long z1 = Util.getZxidFromName(o1.getName(), prefix);
            long z2 = Util.getZxidFromName(o2.getName(), prefix);
            int result = z1 < z2 ? -1 : (z1 > z2 ? 1 : 0);
            return ascending ? result : -result;
        }
    }
    
    /**
     * Sort the list of files. Recency as determined by the version component
     * of the file name.
     *
     * @param files array of files
     * @param prefix files not matching this prefix are assumed to have a
     * version = -1)
     * @param ascending true sorted in ascending order, false results in
     * descending order
     * @return sorted input files
     */
    public static List<File> sortDataDir(File[] files, String prefix, boolean ascending)
    {
        if(files==null)
            return new ArrayList<File>(0);
        List<File> filelist = Arrays.asList(files);
        // ascending 若是 true, 则是按 zxid 升序进行排序
        Collections.sort(filelist, new DataDirFileComparator(prefix, ascending)); // 排序 snapshotFile, 主要以文件名中的 zxid 来进行排序
        return filelist;
    }
    
}

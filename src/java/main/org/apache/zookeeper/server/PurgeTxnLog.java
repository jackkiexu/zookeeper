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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.Util;

/**
 * this class is used to clean up the 
 * snapshot and data log dir's. This is usually
 * run as a cronjob on the zookeeper server machine.
 * Invocation of this class will clean up the datalogdir
 * files and snapdir files keeping the last "-n" snapshot files
 * and the corresponding logs.
 */
public class PurgeTxnLog {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeTxnLog.class);

    static void printUsage(){
        System.out.println("PurgeTxnLog dataLogDir [snapDir] -n count");
        System.out.println("\tdataLogDir -- path to the txn log directory");
        System.out.println("\tsnapDir -- path to the snapshot directory");
        System.out.println("\tcount -- the number of old snaps/logs you want to keep");
        System.exit(1);
    }
    
    /**
     * purges the snapshot and logs keeping the last num snapshots 
     * and the corresponding logs.
     * @param dataDir the dir that has the logs
     * @param snapDir the dir that has the snapshots
     * @param num the number of snapshots to keep
     * @throws IOException
     */
    public static void purge(File dataDir, File snapDir, int num) throws IOException {
        if (num < 3) {
            throw new IllegalArgumentException("count should be greater than 3");
        }

        FileTxnSnapLog txnLog = new FileTxnSnapLog(dataDir, snapDir);                   // 封装 FileTxnSnapLog 对象
        
        // found any valid recent snapshots?
        
        // files to exclude from deletion
        Set<File> exc=new HashSet<File>();
        List<File> snaps = txnLog.findNRecentSnapshots(num);                            // 找出最新的 num 个 snapshot 文件 (根据 zxid, snapshot 的文件名是用 zxid 来进行命名), 这里利用 zxid 来进行倒序排序
        if (snaps.size() == 0) 
            return;
        File snapShot = snaps.get(snaps.size() -1);
        for (File f: snaps) {                                                          // 将最新的 num 个snapshot 文件加入 exc(排除删除的)
            exc.add(f);
        }
        long zxid = Util.getZxidFromName(snapShot.getName(),"snapshot");              // 获取最久的一个 snapshot 文件的 zxid (这里的  zxid 是开始进行 takesnap 时记录的第一个 txnLog, 其实就是保留下来的最小的 txnlog )
        exc.addAll(Arrays.asList(txnLog.getSnapshotLogs(zxid)));                        // 通过 snapshot 的 zxid 来获取 要保留的 txn log 文件, 加入到 exc 里面

        final Set<File> exclude = exc;
        class MyFileFilter implements FileFilter{
            private final String prefix;
            MyFileFilter(String prefix){
                this.prefix=prefix;
            }
            public boolean accept(File f){
                if(!f.getName().startsWith(prefix) || exclude.contains(f))                          // 构造过滤器, 来过滤要删除的文件
                    return false;
                return true;
            }
        }
        // add all non-excluded log files
        List<File> files=new ArrayList<File>(
                Arrays.asList(txnLog.getDataDir().listFiles(new MyFileFilter("log."))));            // 过滤出 要删除的 txn log 文件
        // add all non-excluded snapshot files to the deletion list
        files.addAll(Arrays.asList(txnLog.getSnapDir().listFiles(new MyFileFilter("snapshot.")))); // 过滤出要删除的 snapshot 文件
        // remove the old files
        for(File f: files)                                                                          // 对要删除的文件进行删除
        {
            System.out.println("Removing file: "+
                DateFormat.getDateTimeInstance().format(f.lastModified())+
                "\t"+f.getPath());
            if(!f.delete()){                                // 进行文件的删除
                System.err.println("Failed to remove "+f.getPath());
            }
        }

    }
    
    /**
     * @param args PurgeTxnLog dataLogDir
     *     dataLogDir -- txn log directory
     *     -n num (number of snapshots to keep)
     */
    public static void main(String[] args) throws IOException {
        if(args.length<3 || args.length>4)
            printUsage();
        int i = 0;
        File dataDir=new File(args[0]);
        File snapDir=dataDir;
        if(args.length==4){
            i++;
            snapDir=new File(args[i]);
        }
        i++; i++;
        int num = Integer.parseInt(args[i]);
        purge(dataDir, snapDir, num);
    }
}

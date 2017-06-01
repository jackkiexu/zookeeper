package org.apache.zookeeper.recipes.lock;

/**
 * http://blog.csdn.net/peace1213/article/details/52571445
 * Created by xjk on 6/1/17.
 */
public class DistributedLockTest {

    public static void main(String[] args) {
        DistributedLock lock   = new DistributedLock("127.0.0.1:2181","lock");
        lock.lock();
        //共享资源
        if(lock != null)lock.unlock();
    }

}

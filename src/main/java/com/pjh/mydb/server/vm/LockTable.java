package com.pjh.mydb.server.vm;

import com.pjh.mydb.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 * @author Joseph Peng
 * @date 2022/8/2 18:35
 */
public class LockTable {

    private Map<Long, List<Long>> t2u;  // 某个TID已经获得的资源的UID列表
    private Map<Long, Long> u2t;        // UID被某个TID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的TID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的TID的锁
    private Map<Long, Long> waitU;      // TID正在等待的UID
    private Lock lock;

    public LockTable() {
        t2u = new HashMap<>();
        u2t = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 在每次出现等待的情况时，就尝试向图中增加一条边，并进行死锁检测。
     * 如果检测到死锁，就撤销这条边，不允许添加，并撤销该事务。
     *
     *
     * 不需要等待则返回null，否则返回锁对象
     * 会造成死锁则抛出异常
     */
    public Lock add(long tid, long uid) throws Exception{
        lock.lock();
        try {
            // 如果需要添加的资源已经在当前事务持有的资源的集合中了
            if(isInList(t2u, tid, uid)) {
                return null;
            }

            // 如果没有被其他事务占有，直接获取就好
            if(!u2t.containsKey(uid)){
                u2t.put(uid, tid);
                putIntoList(t2u, tid, uid);
                return null;
            }

            // 否则需要等待，加入到等待集合中，尝试在waitU和wait中加入一条边
            waitU.put(tid, uid);
            putIntoList(wait, uid, tid);
            // 进行死锁检测
            if(hasDeadLock()){
                // 有死锁，撤销加入的边
                waitU.remove(tid);
                removeFromList(wait, uid, tid);
                throw Error.DeadlockException;
            }
            /**
             * 如果需要等待的话，会返回一个上了锁的 Lock 对象。调用方在获取到该对象时，需要尝试获取该对象的锁，
             * 由此实现阻塞线程的目的
             * Lock l = lt.add(xid, uid);
             * if(l != null) {
             *     l.lock();   // 阻塞在这一步
             *     l.unlock();
             * }
             */
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(tid, l);
            return l;
        }finally {
            lock.unlock();
        }
    }

    /**
     * 一个事务 commit 或者 abort 时，就可以释放所有它持有的锁，并将自身从等待图中删除。
     * @param tid
     */
    public void remove(long tid) {
        lock.lock();
        try {
            List<Long> list = t2u.get(tid);
            if(list != null){
                // while 循环释放掉了这个线程所有持有的资源的锁，这些资源可以被等待的线程所获取
                while (list.size() > 0){
                    Long uid = list.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(tid);
            t2u.remove(tid);
            waitLock.remove(tid);
        }finally {
            lock.unlock();
        }
    }

    /**
     * 从等待队列中选择一个tid来占用uid
     * @param uid
     */
    private void selectNewXID(Long uid) {
        u2t.remove(uid);
        List<Long> list = wait.get(uid);
        if (list == null) return;
        while (list.size() > 0){
            long tid = list.remove(0);
            if(!waitLock.containsKey(tid)){
                continue;
            }else {
                u2t.put(uid, tid);
                Lock lo = waitLock.remove(tid);
                waitU.remove(tid);
                lo.unlock();
                break;
            }
        }
        if(list.size() == 0) wait.remove(uid);
    }

    private Map<Long, Integer> tidStamp;
    private int stamp;

    /**
     * 查找图中是否有环
     *
     * 为每个节点设置一个访问戳，都初始化为 -1，随后遍历所有节点，以每个非 -1 的节点作为根进行深搜，
     * 并将深搜该连通图中遇到的所有节点都设置为同一个数字，不同的连通图数字不同。这样，如果在遍历某个图时，
     * 遇到了之前遍历过的节点，说明出现了环。
     *
     * @return
     */
    private boolean hasDeadLock() {
        tidStamp = new HashMap<>();
        stamp = 1;
        for(long tid : t2u.keySet()) {
            Integer s = tidStamp.get(tid);
            // 已经访问过，跳过
            if (s != null && s > 0) continue;
            stamp++;
            if(dfs(tid)){
                return true;
            }
        }
        return false;
    }

    /**
     * 1. 首先检查当前遍历的事务id是否已经被访问过，且是不是当前这一轮dfs访问的，
     *    如果是则有环，否则这一轮dfs可以直接结束，因为当前的事务id已经被遍历过了；
     * 2. 否则就是当前遍历的事务id还未被访问过，那么
     *    1) 设置访问标志
     *    2) 获取当前tid请求的资源uid，根据uid获取持有当前uid的事务id
     *    3) 对获得的uid继续进行dfs
     * 总结：每一次都是从当前事务id，请求的资源id找到持有该资源的事务id，如果遍历完成没有发现
     * @param tid
     * @return
     */
    private boolean dfs(long tid) {
        Integer stp = tidStamp.get(tid);
        // 再次访问，而且还是当前dfs中访问的，说明有环
        if (stp != null && stp == stamp){
            return true;
        }

        // 之前遍历过，非当前的dfs，无环
        if (stp != null && stp < stamp){
            return false;
        }

        // 未访问过，设置dfs的序号
        tidStamp.put(tid, stamp);

        // 获取tid等待的资源序号
        Long uid = waitU.get(tid);
        // 如果没有要等待的资源，返回false
        if (uid == null) return false;
        // 获取持有tid请求的资源的事务id
        Long x = u2t.get(uid);
        assert x != null;
        return dfs(x);
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> list = listMap.get(uid0);
        if (list == null) return;
        Iterator<Long> iterator = list.iterator();
        while (iterator.hasNext()){
            long e = iterator.next();
            if (e == uid1){
                iterator.remove();
                break;
            }
        }
        if (list.size() == 0){
            listMap.remove(uid0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> t2u, long tid, long uid) {
        if(!t2u.containsKey(tid)) {
            t2u.put(tid, new ArrayList<>());
        }
        t2u.get(tid).add(0, uid);
    }

    private boolean isInList(Map<Long, List<Long>> t2u, long tid, long uid) {
        List<Long> list = t2u.get(tid);
        if (list == null) return false;
        return list.contains(uid);
    }

}

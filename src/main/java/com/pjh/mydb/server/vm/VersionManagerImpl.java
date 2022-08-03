package com.pjh.mydb.server.vm;

import com.pjh.mydb.common.Error;
import com.pjh.mydb.server.common.AbstractCache;
import com.pjh.mydb.server.dm.DataManager;
import com.pjh.mydb.server.tm.TransactionManager;
import com.pjh.mydb.server.tm.TransactionManagerImpl;
import com.pjh.mydb.server.utils.Panic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Joseph Peng
 * @date 2022/8/2 12:28
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager{

    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm){
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_TID, Transaction.newTransaction(TransactionManagerImpl.SUPER_TID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }


    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    protected Entry getForCache(long key) throws Exception {
        Entry entry = Entry.loadEntry(this, key);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

    @Override
    public byte[] read(long tid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(tid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        }catch (Exception e){
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }

        try {
            if (Visibility.isVisible(tm, t, entry)){
                return entry.data();
            }else {
                return null;
            }
        }finally {
            entry.release();
        }
    }

    @Override
    public long insert(long tid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(tid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(tid, data);
        return dm.insert(tid, raw);
    }

    @Override
    public boolean delete(long tid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(tid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        }catch (Exception e){
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }

        try {
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                l = lt.add(tid, uid);
            }catch (Exception e){
                t.err = Error.ConcurrentUpdateException;
                internAbort(tid, true);
                t.autoAborted = true;
                throw t.err;
            }

            if(l != null) {
                /**
                 * l.lock()和l.unlock()应该是因为通过add()方法返回的xid的锁是锁定状态的，
                 * 此时是xid对应的事务在等待资源，然后等到xid等待的资源获取到之后，这个锁会unlock()。
                 * 所以在xid对应的事务还没获取到资源之前执行l.lock()会一直阻塞直到该事务获取到资源才能执行l.lock()。
                 * 这里l.lock()只是判断事务有没有获取到资源，获取到之后成功执行，然后就需要unlock()了
                 */
                l.lock();
                l.unlock();
            }

            if(entry.getXmax() == tid) {
                return false;
            }

            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(tid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(tid);

            return true;
        }finally {
            entry.release();
        }
    }

    /**
     * begin() 开启一个事务，并初始化事务的结构，将其存放在 activeTransaction 中，用于检查和快照使用
     * @param level
     * @return
     */
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long tid = tm.begin();
            Transaction t = Transaction.newTransaction(tid, level, activeTransaction);
            activeTransaction.put(tid, t);
            return tid;
        }finally {
            lock.unlock();
        }
    }

    /**
     * commit() 方法提交一个事务，主要就是 free 掉相关的结构，并且释放持有的锁，并修改 TM 状态
     * @param tid
     * @throws Exception
     */
    @Override
    public void commit(long tid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(tid);
        lock.unlock();
        try {
            if (t.err != null){
                throw t.err;
            }
        }catch (NullPointerException e){
            System.out.println(tid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(e);
        }
        lock.lock();
        activeTransaction.remove(tid);
        lock.unlock();
        lt.remove(tid);
        tm.commit(tid);
    }

    @Override
    public void abort(long tid) {
        internAbort(tid, false);
    }

    /**
     * abort 事务的方法则有两种，手动和自动。手动指的是调用 abort() 方法，而自动，则是在事务被检测出出现死锁时，
     * 会自动撤销回滚事务；或者出现版本跳跃时，也会自动回滚
     * @param tid
     * @param autoAborted
     */
    private void internAbort(long tid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(tid);
        if (!autoAborted){
            activeTransaction.remove(tid);
        }
        lock.unlock();
        if (t.autoAborted) return;
        lt.remove(tid);
        tm.abort(tid);
    }
}

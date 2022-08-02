package com.pjh.mydb.server.vm;

import com.pjh.mydb.server.common.AbstractCache;
import com.pjh.mydb.server.dm.DataManager;
import com.pjh.mydb.server.tm.TransactionManager;

import java.util.Map;
import java.util.concurrent.locks.Lock;

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
    }


    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    protected Entry getForCache(long key) throws Exception {
        return null;
    }

    @Override
    protected void releaseForCache(Entry obj) {

    }
}

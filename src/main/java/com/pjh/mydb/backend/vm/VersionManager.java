package com.pjh.mydb.backend.vm;

import com.pjh.mydb.backend.dm.DataManager;
import com.pjh.mydb.backend.tm.TransactionManager;

/**
 * VM 层通过 VersionManager 接口，向上层提供功能
 */
public interface VersionManager {

    byte[] read(long tid, long uid) throws Exception;
    long insert(long tid, byte[] data) throws Exception;
    boolean delete(long tid, long uid) throws Exception;

    long begin(int level);
    void commit(long tid) throws Exception;
    void abort(long tid);

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}

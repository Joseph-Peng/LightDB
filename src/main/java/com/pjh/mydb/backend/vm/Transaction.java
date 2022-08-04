package com.pjh.mydb.backend.vm;

import com.pjh.mydb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 *  vm对一个事务的抽象
 * @author Joseph Peng
 * @date 2022/8/2 16:50
 */
public class Transaction {
    public long tid;
    /**
     * 0 RC
     * 1 RR
     */
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    /**
     *
     * @param tid
     * @param level
     * @param activate 保存着当前所有 active 的事务
     * @return
     */
    public static Transaction newTransaction(long tid, int level, Map<Long, Transaction> activate){
        Transaction t = new Transaction();
        t.tid = tid;
        t.level = level;
        if(level != 0){
            t.snapshot = new HashMap<>();
            for(Long x : activate.keySet()){
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long tid) {
        if (tid == TransactionManagerImpl.SUPER_TID){
            return false;
        }
        return snapshot.containsKey(tid);
    }
}

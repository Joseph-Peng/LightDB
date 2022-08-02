package com.pjh.mydb.server.vm;

import com.pjh.mydb.server.tm.TransactionManager;

/**
 * @author Joseph Peng
 * @date 2022/8/2 17:00
 */
public class Visibility {
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if (t.level == 0){
            return readCommitted(tm, t, e);
        }else{
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 可重复读
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long tid = t.tid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == tid && xmax == 0) return true;

        // xmin为提交状态，且在tid启动之前已提交
        if(tm.isCommitted(xmin) && xmin < tid && !t.isInSnapshot(xmin)){
            // 未被删除
            if (xmax == 0) return true;
            // 由其他事物删除
            if (xmax != tid){
                // 但是 这个事务尚未提交 或 这个事务在Ti开始之后才开始 或  这个事务在Ti开始前还未提交
                if(!tm.isCommitted(xmax) || xmax > tid || t.isInSnapshot(xmax)){
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 读已提交
     * Tid 为 Ti的事务在以下情况可以读取某一条数据
     * 1. 该数据由Ti创建，且还未被删除，可以读取
     * 2. 该数据不是Ti创建，由一个已提交的事务创建，且，还未被删除或者由一个未提交的事务删除(且该事物不是Ti)
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long tid = t.tid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == tid && xmax == 0) return true;

        if(tm.isCommitted(xmin)){
            if (xmax == 0) return true;
            if (xmax != tid && !tm.isCommitted(xmax)){
                return true;
            }
        }
        return false;
    }
}

package com.pjh.mydb.server.dm;

import com.google.common.primitives.Bytes;
import com.pjh.mydb.server.common.SubArray;
import com.pjh.mydb.server.dm.dataitem.DataItem;
import com.pjh.mydb.server.dm.logger.Logger;
import com.pjh.mydb.server.dm.page.Page;
import com.pjh.mydb.server.dm.page.PageX;
import com.pjh.mydb.server.dm.pageCache.PageCache;
import com.pjh.mydb.server.utils.Parser;
import com.pjh.mydb.server.tm.TransactionManager;
import com.pjh.mydb.server.utils.Panic;

import java.util.*;

/**
 * 恢复策略
 * 1. 重做所有崩溃时已完成（committed 或 aborted）的事务
 * 2. 撤销所有崩溃时未完成（active）的事务
 * @author Joseph Peng
 * @date 2022/7/30 0:17
 */
public class Recover {
    /**
     * insert 类型
     * [LogType] [TID] [PageNo] [Offset] [Raw]
     */
    private static final byte LOG_TYPE_INSERT = 0;
    /**
     * update 类型
     * [LogType] [TID] [UID] [OldRaw] [NewRaw]
     */
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    /**
     * (Ti, I, A, x)，表示事务 Ti 在 A 位置插入了一条数据 x
     */
    static class InsertLogInfo {
        long tid;
        int pageNo;
        short offset;
        byte[] raw;
    }

    /**
     * (Ti, U, A, oldx, newx)，表示事务 Ti 将 A 位置的数据，从 oldx 更新成 newx
     */
    static class UpdateLogInfo {
        long tid;
        int pageNo;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        lg.rewind();
        int maxPageNo = 0;
        while (true){
            byte[] log = lg.next();
            if (log == null) break;
            int pageNo;
            if(isInsertLog(log)){
                InsertLogInfo logInfo = parseInsertLog(log);
                pageNo = logInfo.pageNo;
            }else{
                UpdateLogInfo logInfo = parseUpdateLog(log);
                pageNo = logInfo.pageNo;
            }
            maxPageNo = Math.max(maxPageNo, pageNo);
        }
        if (maxPageNo == 0){
            maxPageNo = 1;
        }
        pc.truncateByPageNo(maxPageNo);
        System.out.println("Truncate to " + maxPageNo + " pages.");

        redoTransactions(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        undoTransactions(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");

    }

    private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while (true){
            byte[] log = lg.next();
            if (log == null) break;
            if(isInsertLog(log)){
                InsertLogInfo li = parseInsertLog(log);
                long tid = li.tid;
                if(tm.isActive(tid)){
                    doInsertLog(pc, log, REDO);
                }
            }else{
                UpdateLogInfo xi = parseUpdateLog(log);
                long tid = xi.tid;
                if(!tm.isActive(tid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    private static void undoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while (true){
            byte[] log = lg.next();
            if (log == null) break;
            if(isInsertLog(log)){
                InsertLogInfo li = parseInsertLog(log);
                long tid = li.tid;
                if(tm.isActive(tid)){
                    if(!logCache.containsKey(tid)){
                        logCache.put(tid, new ArrayList<>());
                    }
                    logCache.get(tid).add(log);
                }
            }else{
                UpdateLogInfo xi = parseUpdateLog(log);
                long tid = xi.tid;
                if(tm.isActive(tid)) {
                    if(!logCache.containsKey(tid)) {
                        logCache.put(tid, new ArrayList<>());
                    }
                    logCache.get(tid).add(log);
                }
            }
        }

        // 对所有active log进行倒序undo
        for(Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for(int i = logs.size() - 1; i >= 0; --i){
                byte[] log = logs.get(i);
                if (isInsertLog(log)){
                    doInsertLog(pc, log, UNDO);
                }else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }


    private static final int OF_TYPE = 0;
    private static final int OF_TID = OF_TYPE+1;

    // [LogType] [TID] [UID] [OldRaw] [NewRaw]  更新
    private static final int OF_UPDATE_UID = OF_TID+8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;

    // [LogType] [TID] [PageNo] [Offset] [Raw] 插入
    private static final int OF_INSERT_PAGE_NO = OF_TID + 8; // 页号
    private static final int OF_INSERT_OFFSET = OF_INSERT_PAGE_NO + 4;// 页内偏移
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2; // 数据在页内的位置

    // [LogType] [TID] [UID] [OldRaw] [NewRaw]
    public static byte[] updateLog(long tid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2byte(tid);
        byte[] uidRaw = Parser.long2byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log){
        UpdateLogInfo logInfo = new UpdateLogInfo();
        logInfo.tid = Parser.parseLong(Arrays.copyOfRange(log, OF_TID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        logInfo.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        logInfo.pageNo =  (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        logInfo.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        logInfo.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return logInfo;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int type) {
        int pageNo;
        short offset;
        byte[] raw;
        UpdateLogInfo logInfo = parseUpdateLog(log);
        pageNo = logInfo.pageNo;
        offset = logInfo.offset;
        if(type == REDO){
            raw = logInfo.newRaw;
        }else{
            raw = logInfo.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pageNo);
        }catch (Exception e){
            Panic.panic(e);
        }

        try{
            PageX.recoverUpdate(pg, raw, offset);
        }finally {
            pg.release();
        }
    }

    public static byte[] insertLog(long tid, Page pg, byte[] raw) {
        byte[] logType = {LOG_TYPE_INSERT};
        byte[] logTid = Parser.long2byte(tid);
        byte[] logPageNo = Parser.int2byte(pg.getPageNumber());
        byte[] logOffset = Parser.short2byte(PageX.getFSO(pg));
        return Bytes.concat(logType, logTid, logPageNo, logOffset, raw);
    }

    /**
     * [LogType] [TID] [PageNo] [Offset] [Raw] 插入
     * @param log
     * @return
     */
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo info = new InsertLogInfo();
        info.tid = Parser.parseLong(Arrays.copyOfRange(log, OF_TID, OF_INSERT_PAGE_NO));
        info.pageNo = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PAGE_NO, OF_INSERT_OFFSET));
        info.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        info.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return info;
    }

    /**
     * redo : 将log重新插入
     * undo : 将log删除，逻辑删除
     * @param pc
     * @param log
     * @param type
     */
    private static void doInsertLog(PageCache pc, byte[] log, int type) {
        InsertLogInfo info = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(info.pageNo);
        }catch (Exception e){
            Panic.panic(e);
        }

        try {
            if (type == UNDO){
                // 逻辑删除
                DataItem.setDataItemRawInvalid(info.raw);
            }
            PageX.recoverInsert(pg, info.raw, info.offset);
        }finally {
            pg.release();
        }
    }



    /**
     *
     * @param log
     * @return
     */
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }


}

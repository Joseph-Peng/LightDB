package com.pjh.mydb.server.vm;

import com.google.common.primitives.Bytes;
import com.pjh.mydb.server.common.SubArray;
import com.pjh.mydb.server.dm.dataitem.DataItem;
import com.pjh.mydb.server.utils.Parser;

import java.util.Arrays;

/**
 *
 *  VM向上层抽象出entry
 *  entry结构：
 *  [XMIN] [XMAX] [data]
 * XMIN 是创建该条记录（版本）的事务编号，而 XMAX 则是删除该条记录（版本）的事务编号。
 *
 *  ValidFlag|Size| Data  Data为[XMIN] [XMAX] [data]
 *
 * @author Joseph Peng
 * @date 2022/8/2 15:23
 */
public class Entry {

    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN + 8;
    private static final int OF_DATA = OF_XMAX + 8;

    private long uid;
    private DataItem dataItem;
    private VersionManager vm;

    public static Entry loadEntry(VersionManager vm, long uid) throws Exception{
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    public void remove(){
        dataItem.release();
    }

    /**
     * 创建一个Entry记录
     * @param tid
     * @param data
     * @return
     */
    public static byte[] wrapEntryRaw(long tid, byte[] data) {
        byte[] xmin = Parser.long2Byte(tid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    /**
     * 获取Entry类型中的数据-->[XMIN] [XMAX] [data]
     * 以拷贝的形式返回内容
     * @return
     */
    public byte[] data(){
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw, sa.start + OF_DATA, data, 0, data.length);
            return data;
        }finally {
            dataItem.rUnLock();
        }
    }

    public long getXmin(){
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMIN, sa.start + OF_XMAX));
        }finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax(){
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMAX, sa.start+OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * setXMax需要修改dataItem，需要先执行before，再执行after
     * @param tid
     * @return
     */
    public void setXmax(long tid){
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(tid), 0, sa.raw, sa.start+OF_XMAX, 8);
        }finally {
            // 对修改落日志
            dataItem.after(tid);
        }
    }

    public long getUid() {
        return uid;
    }

}

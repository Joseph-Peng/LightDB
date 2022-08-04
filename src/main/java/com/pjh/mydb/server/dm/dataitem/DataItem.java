package com.pjh.mydb.server.dm.dataitem;

import com.google.common.primitives.Bytes;
import com.pjh.mydb.server.common.SubArray;
import com.pjh.mydb.server.dm.DataManagerImpl;
import com.pjh.mydb.server.dm.page.Page;
import com.pjh.mydb.server.utils.Parser;
import com.pjh.mydb.server.utils.Types;

import java.util.Arrays;

/**
 * @author Joseph Peng
 * @date 2022/8/1 16:16
 */
public interface DataItem {

    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    /**
     * 用于逻辑删除
     * @param raw
     */
    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }

    /**
     * 将数据包装为dataItem格式 [valid][size][data] 1 2
     * @param raw
     * @return
     */
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    /**
     * 从页面的offset处解析处DataItem
     * @param pg  DataItem所在页面
     * @param offset 页内偏移
     * @param dm  DM
     * @return
     */
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm){
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        // 头部加上数据的长度  3 + size
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        // 将这一段数据放入DataItem
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], pg, uid, dm);
    }

}

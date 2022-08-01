package com.pjh.mydb.server.dm.dataitem;

import com.pjh.mydb.server.common.SubArray;
import com.pjh.mydb.server.dm.page.Page;

/**
 * @author Joseph Peng
 * @date 2022/8/1 16:16
 */
public interface DataItem {

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }
}

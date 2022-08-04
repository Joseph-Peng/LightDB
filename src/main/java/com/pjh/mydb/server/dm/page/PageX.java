package com.pjh.mydb.server.dm.page;

import com.pjh.mydb.server.dm.pageCache.PageCache;
import com.pjh.mydb.server.utils.Parser;

import java.util.Arrays;

/**
 * 普通页管理
 * 普通页面以一个 2 字节无符号数起始，表示这一页的空闲位置的偏移
 * 一页的大小为8KB， 2个字节完全可以表示
 * @author Joseph Peng
 * @date 2022/7/29 15:52
 */
public class PageX {

    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    // 最大空闲空间
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        // 将2写入到 raw的0-2字节的位置
        setFSO(raw, OF_DATA);
        return raw;
    }

    /**
     * 设置剩余空间的偏移位置
     * @param raw
     * @param ofData
     */
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    /**
     * 获取页面的空闲位置偏移
     * @param page
     * @return
     */
    public static short getFSO(Page page){
        return getFSO(page.getData());
    }

    public static short getFSO(byte[] data){
        return Parser.parseShort(Arrays.copyOfRange(data, 0, 2));
    }

    /**
     * 获取页面的空闲空间大小
     * @param page
     * @return
     */
    public static int getFreeSpace(Page page){
        return PageCache.PAGE_SIZE - (int)getFSO(page);
    }

    /**
     * 将raw中的数据插入到page中，并返回插入的位置
     * @param page
     * @param raw
     * @return
     */
    public static short insert(Page page, byte[] raw){
        //在写入之前获取 FSO，来确定写入的位置，并在写入之后更新 FSO。
        page.setDirty(true);
        short offset = getFSO(page.getData());
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        setFSO(page.getData(), (short)(offset + raw.length));
        return offset;
    }


    //recoverInsert() 和 recoverUpdate() 用于在数据库崩溃后重新打开时，
    // 恢复过程直接插入数据以及修改数据使用。
    /**
     * 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
     * @param pg
     * @param raw
     * @param offset
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length){
            setFSO(pg.getData(), (short) (offset + raw.length));
        }
    }

    /**
     *  将raw插入pg中的offset位置，不更新update
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }

}

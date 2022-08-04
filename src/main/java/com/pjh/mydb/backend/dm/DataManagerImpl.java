package com.pjh.mydb.backend.dm;

import com.pjh.mydb.common.Error;
import com.pjh.mydb.backend.common.AbstractCache;
import com.pjh.mydb.backend.dm.dataitem.DataItem;
import com.pjh.mydb.backend.dm.dataitem.DataItemImpl;
import com.pjh.mydb.backend.dm.logger.Logger;
import com.pjh.mydb.backend.dm.page.Page;
import com.pjh.mydb.backend.dm.page.PageOne;
import com.pjh.mydb.backend.dm.page.PageX;
import com.pjh.mydb.backend.dm.pageCache.PageCache;
import com.pjh.mydb.backend.dm.pageindex.PageIndex;
import com.pjh.mydb.backend.dm.pageindex.PageInfo;
import com.pjh.mydb.backend.tm.TransactionManager;
import com.pjh.mydb.backend.utils.Panic;
import com.pjh.mydb.backend.utils.Types;

/**
 * DataManager 是 DM 层直接对外提供方法的类，同时，也实现成 DataItem 对象的缓存。
 * DataItem 存储的 key，是由页号和页内偏移组成的一个 8 字节无符号整数，页号和偏移各占 4 字节。
 * @author Joseph Peng
 * @date 2022/8/1 18:26
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    // 初始化pageIndex
    void fillPageIndex() {
        // 获取总页数
        int pageNumbers = pc.getPageNumber();
        for(int i = 2; i <= pageNumbers; ++i){
            Page pg = null;
            try {
                pg = pc.getPage(i);
            }catch (Exception e){
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            //使用完 Page 后需要及时 release，否则可能会撑爆缓存。
            pg.release();
        }
    }

    public void logDataItem(long tid, DataItemImpl dataItem) {
        byte[] log = Recover.updateLog(tid, dataItem);
        logger.log(log);
    }

    public void releaseDataItem(DataItemImpl dataItem) {
        super.release(dataItem.getUid());
    }

    /**
     * 从uid中解析出页号，再用PageCache中获取页面，根据偏移解析出DataItem
     * @param key 页号+偏移
     * @return
     * @throws Exception
     */
    @Override
    protected DataItem getForCache(long key) throws Exception {
        short offset = (short)(key & ((1L << 16) - 1));
        key >>>= 32;
        int pageNo =  (int)(key & ((1L << 32) - 1));
        Page pg = pc.getPage(pageNo);
        return DataItem.parseDataItem(pg, offset, this);
    }

    /**
     * DataItem 缓存释放，需要将 DataItem 写回数据源，由于对文件的读写是以页为单位进行的，
     * 只需要将 DataItem 所在的页 release即可。
     * @param di
     */
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    /**
     * 根据 UID 从缓存中获取 DataItem，并校验有效位
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl dataItem = (DataItemImpl)super.get(uid);
        if (!dataItem.isValid()){
            dataItem.release();
            return null;
        }
        return dataItem;
    }

    /**
     * 1. 在 pageIndex 中获取一个足以存储插入内容的页面的页号，获取页面后，首先需要写入插入日志，
     * 2. 接着才可以通过 pageX 插入数据，并返回插入位置的偏移。
     * 3. 最后需要将页面信息重新插入 pageIndex。
     * @param tid
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long tid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        // 数据太大
        if (raw.length > PageX.MAX_FREE_SPACE){
            throw Error.DataTooLargeException;
        }

        // 从pageIndex中获取足够插入的页面页号
        PageInfo pageInfo = null;
        for(int i = 0; i < 5; ++i){
            pageInfo = pIndex.select(raw.length);
            if (pageInfo != null){
                break;
            }else {
                int newPageNo = pc.newPage(PageX.initRaw());
                pIndex.add(newPageNo, PageX.MAX_FREE_SPACE);
            }
        }

        if (pageInfo == null){
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            // 获取缓存页面
            pg = pc.getPage(pageInfo.pageNo);
            byte[] log = Recover.insertLog(tid, pg, raw);
            // 先写入日志
            logger.log(log);

            // 写入数据
            short offset = PageX.insert(pg, raw);

            // 用完释放缓存
            pg.release();
            return Types.addressToUid(pageInfo.pageNo, offset);
        }finally {
            // 将取出的page重新插入pIndex
            if (pg != null){
                pIndex.add(pageInfo.pageNo, PageX.getFreeSpace(pg));
            }else {
                pIndex.add(pageInfo.pageNo, freeSpace);
            }
        }
    }

    /**
     * 关闭DM，执行缓存和日志的关闭，并设置第一页的字节校验
     */
    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    /**
     * 在创建文件时初始化PageOne
     */
    public void initPageOne() {
        int pageNo = pc.newPage(PageOne.initRaw());
        assert pageNo == 1;
        try {
            pageOne = pc.getPage(pageNo);
        }catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    /**
     * 在打开已有文件时时读入PageOne，并验证正确性
     * @return
     */
    public boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        }catch (Exception e){
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }
}

package com.pjh.mydb.server.dm;

import com.pjh.mydb.server.dm.dataitem.DataItem;
import com.pjh.mydb.server.dm.logger.Logger;
import com.pjh.mydb.server.dm.page.PageOne;
import com.pjh.mydb.server.dm.pageCache.PageCache;
import com.pjh.mydb.server.tm.TransactionManager;

public interface DataManager {

    DataItem read(long uid) throws Exception;
    long insert(long tid, byte[] data) throws Exception;
    void close();

    /**
     * 从空文件创建
     * @param path
     * @param mem
     * @param tm
     * @return
     */
    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    /**
     * 已有文件加载
     * @param path
     * @param mem
     * @param tm
     * @return
     */
    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);

        if(!dm.loadCheckPageOne()){
            // 上一次非正常关闭
            Recover.recover(tm, lg, pc);
        }

        // 初始化每一页的空闲空间列表
        dm.fillPageIndex();
        // 初始化PageOne
        PageOne.setVcOpen(dm.pageOne);
        // 将PageOne写会db文件
        dm.pc.flushPage(dm.pageOne);
        return dm;
    }
}

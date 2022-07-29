package com.pjh.mydb.server.dm.page;

import com.pjh.mydb.server.dm.pageCache.PageCache;
import com.pjh.mydb.server.dm.pageCache.PageCacheImpl;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页对象
 * @author Joseph Peng
 * @date 2022/7/28 19:24
 */
public class PageImpl implements Page{
    /**
     * 当前页面的页号，页号从1开始
     */
    private int pageNumber;

    /**
     * 当前页面包含的实际数据
     */
    private byte[] data;

    /**
     * 修改位
     */
    private boolean dirty;

    private Lock lock;

    /**
     *  PageCache的引用，用来方便在拿到 Page 的引用时可以快速对这个页面的缓存进行释放操作
     */
    private PageCache pc;

    public PageImpl(int pageNo, byte[] data, PageCache pageCache) {
        this.pageNumber = pageNo;
        this.data = data;
        this.pc = pageCache;
        lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}

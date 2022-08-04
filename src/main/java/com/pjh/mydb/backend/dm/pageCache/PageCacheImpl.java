package com.pjh.mydb.backend.dm.pageCache;

import com.pjh.mydb.common.Error;
import com.pjh.mydb.backend.common.AbstractCache;
import com.pjh.mydb.backend.dm.page.Page;
import com.pjh.mydb.backend.dm.page.PageImpl;
import com.pjh.mydb.backend.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面缓存的具体实现
 * @author Joseph Peng
 * @date 2022/7/28 19:22
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache{

    /**
     * 最小缓存数量
     */
    private static final int MEM_MIN_LIM = 10;

    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    /**
     * 总页数
     */
    private AtomicInteger pageNumbers;

    PageCacheImpl(RandomAccessFile file, FileChannel fc, int maxResource){
        super(maxResource);
        if (maxResource < MEM_MIN_LIM){
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        }catch (IOException e){
            Panic.panic(e);
        }

        this.file = file;
        this.fc = fc;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }


    @Override
    protected Page getForCache(long key) throws Exception {
        int pageNo = (int) key;
        long offset = pageOffset(pageNo);

        // 从DB文件中读取出一页的数据，并包装成page返回
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try{
            fc.position(offset);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }
        return new PageImpl(pageNo, buf.array(), this);
    }

    private static long pageOffset(int pageNo) {
        return (pageNo - 1) * PAGE_SIZE;
    }
    @Override
    protected void releaseForCache(Page page) {
        if (page.isDirty()){
            flush(page);
            page.setDirty(false);
        }
    }

    private void flush(Page page) {
        int pageNo = page.getPageNumber();
        long offset = pageOffset(pageNo);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }
    }

    @Override
    public int newPage(byte[] initData) {
        int pageNo = pageNumbers.incrementAndGet();
        Page page = new PageImpl(pageNo, initData, null);
        flush(page);
        return pageNo;
    }

    @Override
    public Page getPage(int pageNo) throws Exception {
        return get((long) pageNo);
    }

    @Override
    public void close() {
        super.close();
        try{
            fc.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        release((long) page.getPageNumber());
    }

    @Override
    public void truncateByPageNo(int maxPageNo) {
        long size = pageOffset(maxPageNo + 1);
        try {
            file.setLength(size);
        }catch (IOException e){
            Panic.panic(e);
        }
        pageNumbers.set(maxPageNo);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }
}

package com.pjh.mydb.server.dm.pageCache;

import com.pjh.mydb.common.Error;
import com.pjh.mydb.server.dm.page.Page;
import com.pjh.mydb.server.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @author Joseph Peng
 * @date 2022/7/28 19:21
 */
public interface PageCache {

    /**
     * 默认页的大小为8KB
     */
    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);

    Page getPage(int pageNo) throws Exception;

    void close();

    void release(Page page);

    void truncateByPageNo(int max);

    int getPageNumber();

    void flushPage(Page page);

    /**
     * 在指定路径下创建一个.db文件，并初始化页面缓存管理器
     * @param path  .db文件路径
     * @param memory  分配内存空间大小
     * @return
     */
    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        }catch (Exception e){
            Panic.panic(e);
        }

        if(!f.canRead() || !f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(f,"rw");
            fc = raf.getChannel();
        }catch (FileNotFoundException e){
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }


    /**
     * 启动时打开.db文件，并初始化页面缓存管理器
     * @param path
     * @param memory
     * @return
     */
    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        if(!f.exists()){
            Panic.panic(Error.FileNotExistsException);
        }

        if(!f.canRead() || !f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(f,"rw");
            fc = raf.getChannel();
        }catch (FileNotFoundException e){
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

}

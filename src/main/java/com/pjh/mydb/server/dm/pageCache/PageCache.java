package com.pjh.mydb.server.dm.pageCache;

import com.pjh.mydb.server.dm.page.Page;

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

}

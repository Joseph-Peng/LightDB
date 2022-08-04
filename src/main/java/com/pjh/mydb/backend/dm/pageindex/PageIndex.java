package com.pjh.mydb.backend.dm.pageindex;

import com.pjh.mydb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * 页面索引，缓存了每一页的空闲空间。
 * 用于在上层模块进行插入操作时，能够快速找到一个合适空间的页面，而无需从磁盘或者缓存中检查每一个页面的信息。
 *
 * @author Joseph Peng
 * @date 2022/8/1 17:16
 */
public class PageIndex {

    // 将一页划分为40个区间
    private static final int INTERVALS_NO = 40;
    // 每个区间的最大空间
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    /**
     * 大小为41的数组 存的是区间号（区间号从1>开始），然后每个区间号数组后面跟一个数组存满足空闲大小
     * 	的所有数据页信息（PageInfo）
     */
    private List<PageInfo>[] lists;

    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for(int i = 0; i < INTERVALS_NO + 1; ++i){
            lists[i] = new ArrayList<>();
        }
    }

    /**
     *
     * @param spaceSize 所需的空间大小
     * @return
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 找到空闲合适的区间
            int number = spaceSize / THRESHOLD;
            if(number < INTERVALS_NO) number++;
            while (number <= INTERVALS_NO){
                if (lists[number].size() == 0){
                    number++;
                    continue;
                }
                // 返回合适的页面，并将这个页面的索引移除，防止同时多个线程对同一个页面进行写入
                return lists[number].remove(0);
            }
            return null;
        }finally {
            lock.unlock();
        }
    }

    public void add(int pageNo, int freeSpace) {
        lock.lock();
        try {
            // 页面加入进来时，先计算该页面空闲空间所在的区间，然后加入
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pageNo, freeSpace));
        }finally {
            lock.unlock();
        }
    }
}

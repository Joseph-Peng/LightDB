package com.pjh.mydb.backend.common;

/**
 * @author Joseph Peng
 * @date 2022/7/28 16:43
 */

import com.pjh.mydb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractCache 实现了一个引用计数策略的缓存.
 * getForCache
 * releaseForCache
 * 子类可以通过实现这两个方法来加载数据到缓存中和从缓存中释放数据
 */
public abstract class AbstractCache<T> {
    /**
     * 实际缓存的的数据
     */
    private HashMap<Long, T> cache;

    /**
     * 元素的引用个数
     */
    private HashMap<Long, Integer> references;

    /**
     * 正在获取资源的某个线程
     */
    private HashMap<Long, Boolean> getting;

    /**
     * 缓存的最大缓存资源数
     */
    private int maxResource;

    /**
     * 缓存中元素的个数
     */
    private int count = 0;

    private Lock lock;

    public AbstractCache(int maxResource){
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 根据资源的key获取资源
     * @param key
     * @return
     */
    protected T get(long key) throws Exception {
        System.out.println(key);
        // 死循环，来无限尝试从缓存里获取
        while (true){
            lock.lock();
            // 如果当前有其他线程正在获取这个资源，就先等待一下，过会再来看看
            if(getting.containsKey(key)){
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            // 如果资源在缓存中，直接返回
            if (cache.containsKey(key)){
                T data = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return data;
            }

            // 如果资源既没有被别的线程请求，也不在缓存中，则尝试获取该资源，并放入缓存
            // 缓存满了，类似于OOM
            if (maxResource > 0 && count == maxResource){
                lock.unlock();
                throw Error.CacheFullException;
            }
            // 缓存未满，在getting中注册一下，表明该线程准备从数据源获取资源了。
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        // 缓存中没有，从数据源获取资源
        T data = null;
        try {
            data = getForCache(key);
        }catch (Exception e){
            // 获取出现异常，恢复现场
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        // 获取到数据，更新cache
        lock.lock();
        getting.remove(key);
        cache.put(key, data);
        references.put(key, 1); // 走到这里一定是缓存中没有的情况，所以将引用设置为1
        lock.unlock();

        return data;
    }

    /**
     * 强行释放一个缓存
     */
    protected void release(long key){
        lock.lock();
        try{
            int refCount = references.get(key) - 1;
            if(refCount == 0){
                T data = cache.get(key);
                releaseForCache(data);
                references.remove(key);
                cache.remove(key);
                count--;
            }else if (refCount > 0){
                references.put(key, refCount);
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for(long key : keys){
                T data = cache.get(key);
                releaseForCache(data);
                references.remove(key);
                cache.remove(key);
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);


}

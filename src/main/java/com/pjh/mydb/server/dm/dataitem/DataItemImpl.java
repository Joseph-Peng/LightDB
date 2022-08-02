package com.pjh.mydb.server.dm.dataitem;

import com.pjh.mydb.server.common.SubArray;
import com.pjh.mydb.server.dm.DataManagerImpl;
import com.pjh.mydb.server.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * DataItem 是 DM 层向上层提供的数据抽象。上层模块通过地址，向 DM 请求到对应的 DataItem，再获取到其中的数据。
 * DataItem 中保存的数据，结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag(byte) 占用 1 字节，标识了该 DataItem 是否有效。删除一个 DataItem，只需要简单地将其有效位设置为 0。
 * DataSize(short) 占用 2 字节，标识了后面 Data 的长度。
 *
 * @author Joseph Peng
 * @date 2022/8/1 16:18
 */
public class DataItemImpl implements DataItem{

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    /**
     * 共享内存数组
     */
    private SubArray raw;
    private byte[] oldRaw;
    /**
     * 保存一个 dm 的引用是因为其释放依赖 dm 的释放（dm 同时实现了缓存接口，用于缓存 DataItem），以及修改数据时落日志。
     */
    private DataManagerImpl dm;
    /**
     * DM 缓存 DataItem, key为uid
     * UID记录了页号和页内偏移
     */
    private long uid;
    private Page pg;

    private Lock rLock;
    private Lock wLock;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        this.pg = pg;
        this.uid = uid;
        this.dm = dm;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
    }

    /**
     * 0表示有效，1表示无效
     * @return
     */
    public boolean isValid(){
        return raw.raw[raw.start + OF_VALID] == (byte)0;
    }

    /**
     * 上层模块在获取到 DataItem 后，可以通过 data() 方法，该方法返回的数组是数据共享的，而不是拷贝实现的，
     * 所以使用了 SubArray。
     * @return
     */
    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    /**
     * 在上层模块试图对 DataItem 进行修改时，需要遵循一定的流程：
     * 在修改之前需要调用 before() 方法，想要撤销修改时，调用 unBefore() 方法，
     * 在修改完成后，调用 after() 方法。整个流程，主要是为了保存前相数据，并及时落日志。
     * DM 会保证对 DataItem 的修改是原子性的。
     */

    /**
     * 修改之前需要调用before方法
     */
    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        /** 把一个数组中某一段字节数据放到另一个数组中
         * src:源数组;
         * srcPos:源数组要复制的起始位置;
         * dest:目的数组;
         * destPos:目的数组放置的起始位置;
         * length:复制的长度.
         */
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    /**
     * 撤销修改之前需要调用unBefore方法
     */
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    /**
     * 对修改操作落日志
     * @param tid
     */
    @Override
    public void after(long tid) {
        dm.logDataItem(tid, this);
        wLock.unlock();
    }

    /**
     * 释放掉 DataItem 的缓存（由 DM 缓存 DataItem）
     */
    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}

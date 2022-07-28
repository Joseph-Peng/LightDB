package com.pjh.mydb.server.tm;

import com.pjh.mydb.common.Error;
import com.pjh.mydb.server.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Joseph Peng
 * TM 通过维护 TID 文件来维护事务的状态，并提供接口供其他模块来查询某个事务的状态。
 */
public interface TransactionManager {
    /**
     * 开启一个事物
     * @return Tid：事物id
     */
    long begin();

    /**
     * 提交一个事物
     * @param tid
     */
    void commit(long tid);

    /**
     * 取消一个事务
     * @param tid
     */
    void abort(long tid);

    /**
     * 查询一个事务的状态是否是正在进行的状态
     * @param tid
     * @return 状态
     */
    boolean isActive(long tid);

    /**
     * 查询一个事务的状态是否是已提交
     * @param tid
     * @return
     */
    boolean isCommitted(long tid);

    /**
     * 查询一个事务的状态是否是已取消
     * @param tid
     * @return
     */
    boolean isAborted(long tid);

    /**
     * 关闭TransactionManager
     */
    void close();

    /**
     * 初始化TM
     * @param path
     * @return
     */
    public static TransactionManagerImpl create(String path){
        File tidFile = new File(path + TransactionManagerImpl.TID_SUFFIX);
        try {
            if(!tidFile.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(!tidFile.canRead() || !tidFile.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(tidFile,"rw");
            fc = raf.getChannel();
        }catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 初始化TID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_TID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }

    /**
     * 从一个已有的 tid 文件来创建 TM
     * @param path
     * @return
     */
    public static TransactionManagerImpl open(String path){
        File tidFile = new File(path + TransactionManagerImpl.TID_SUFFIX);
        if (!tidFile.exists()){
            Panic.panic(Error.FileNotExistsException);
        }

        if(!tidFile.canRead() || !tidFile.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(tidFile,"rw");
            fc = raf.getChannel();
        }catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf, fc);
    }
}

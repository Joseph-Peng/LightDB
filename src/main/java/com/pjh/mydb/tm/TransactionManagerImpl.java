package com.pjh.mydb.tm;

import com.pjh.mydb.common.Error;
import com.pjh.mydb.common.Parser;
import com.pjh.mydb.utils.Panic;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Joseph Peng
 * TM 通过维护 TID 文件来维护事务的状态，并提供接口供其他模块来查询某个事务的状态。
 */
public class TransactionManagerImpl implements TransactionManager{


    /**
     * TID文件头长度 : 在TID文件的头部，保存了一个8字节的数字，记录了这个TID文件管理的事务的个数。
     */
    static final int LEN_TID_HEADER_LENGTH = 8;

    /**
     * 每个事务的占用长度: 1 byte
     */
    private static final int TID_FIELD_SIZE = 1;

    /**
     * 事务的三种状态:
     * FIELD_TRAN_ACTIVE : 运行中
     * FIELD_TRAN_COMMITTED：已提交
     * FIELD_TRAN_ABORTED：已撤销(回滚)
     */
    private static final byte FIELD_TRAN_ACTIVE   = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED  = 2;

    /**
     * 超级事务,TID为0，永远为committed状态
     * 当一些操作想在没有申请事务的情况下进行，那么可以将操作的 TID 设置为 0。
     */
    public static final long SUPER_TID = 0;

    /**
     * TID 文件后缀
     */
    static final String TID_SUFFIX = ".tid";

    private RandomAccessFile file;
    private FileChannel fc;
    private long tidCounter;
    private Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc){
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查TID文件是否合法
     * 1. TID文件长度要大于等于LEN_TID_HEADER_LENGTH，也就是至少有文件首部8个字节
     * 2. TID文件中首部8个字节记录了存储事物的数量tidCounter，除去一个超级事物，文件长度为8+事物数量-1；
     *    LEN_TID_HEADER_LENGTH + (tid-1)*TID_FIELD_SIZE == fileLen才合法
     * 对于校验没有通过的，会直接通过 panic 方法，强制停机。
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (Exception e1) {
            Panic.panic(Error.BadTIDFileException);
        }
        if(fileLen < LEN_TID_HEADER_LENGTH) {
            Panic.panic(Error.BadTIDFileException);
        }

        // 分配8个字节的缓冲区
        ByteBuffer buf = ByteBuffer.allocate(LEN_TID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 将首部8字节转为long
        this.tidCounter = Parser.parseLong(buf.array());
        long end = getTidPosition(this.tidCounter + 1);
        if(end != fileLen) {
            Panic.panic(Error.BadTIDFileException);
        }
    }

    // 根据事务xid取得其在xid文件中对应的位置
    private long getTidPosition(long tid) {
        return LEN_TID_HEADER_LENGTH + (tid-1)*TID_FIELD_SIZE;
    }

    @Override
    public long begin() {
        return 0;
    }

    @Override
    public void commit(long tid) {

    }

    @Override
    public void abort(long tid) {

    }

    @Override
    public boolean isActive(long tid) {
        return false;
    }

    @Override
    public boolean isCommitted(long tid) {
        return false;
    }

    @Override
    public boolean isAborted(long tid) {
        return false;
    }

    @Override
    public void close() {

    }
}

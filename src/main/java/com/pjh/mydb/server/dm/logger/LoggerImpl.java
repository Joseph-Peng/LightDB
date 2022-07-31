package com.pjh.mydb.server.dm.logger;

import com.google.common.primitives.Bytes;
import com.pjh.mydb.common.Error;
import com.pjh.mydb.server.parser.Parser;
import com.pjh.mydb.server.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;



/**
 * 日志文件读写
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 *
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度, Checksum 4字节int
 *
 * @author Joseph Peng
 * @date 2022/7/30 0:18
 */
public class LoggerImpl implements Logger{

    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    /**
     * 单个日志中数据的偏移量，Size 4字节int +  Checksum 4字节int = 8字节
     */
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock lock;

    /**
     * 当前日志指针的位置
     */
    private long position;
    /**
     * 初始化时记录， log操作不更新
     */
    private long fileSize;
    private int xChecksum;

    LoggerImpl(RandomAccessFile raf, FileChannel fc){
        this.raf = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.raf = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    /**
     * 单条日志的校验和，其实就是通过一个指定的种子实现的
     * @param xCheck
     * @param log
     * @return
     */
    private int calChecksum(int xCheck, byte[] log){
        for(byte b : log){
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }



    /**
     * 在打开一个日志文件时，需要首先校验日志文件的 XChecksum，并移除文件尾部可能存在的 BadTail，
     * 由于 BadTail 该条日志尚未写入完成，文件的校验和也就不会包含该日志的校验和，去掉 BadTail
     * 即可保证日志文件的一致性。
     *
     * 检查并移除bad tail
     */
    private void checkAndRemoveTail() {
        // 从第一条日志开始
        rewind();
        int xCheck = 0;
        while (true){
            byte[] log = internNext();
            if (log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if (xCheck != xChecksum){
            Panic.panic(Error.BadLogFileException);
        }
        // 校验成功
        try {
            // 移除badtail
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        /*
        try {
            // seek(long a)是定位文件指针在文件中的位置。
            // 参数a确定读写位置距离文件开头的字节个数，比如seek(0)就是定位文件指针在开始位置。
            // 是绝对定位。
            raf.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }*/

        rewind();
    }


    /**
     * 写入日志文件
     * @param data 日志byte数组
     * 首先将数据包裹成日志格式，然后写入文件，再更新检验和，调用fc.force(false),刷新缓冲区，
     * 保证内容写入磁盘。
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            lock.unlock();
        }
        // 写入完成，更新校验和并flush缓存
        updateXChechsum(log);
    }

    private void updateXChechsum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2byte(this.xChecksum)));
            fc.force(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2byte(calChecksum(0, data));
        byte[] size = Parser.int2byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    /**
     * 截断为指定的长度
     * @param x 长度
     * @throws Exception
     */
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        }finally {
            lock.unlock();
        }
    }

    /**
     * 不断地从文件中读取下一条日志，并将其中的 Data 解析出来并返回。next() 方法的实现主要依靠 internNext()
     * @return 日志数据
     */
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        }finally {
            lock.unlock();
        }
    }

    private byte[] internNext() {
        // 超出文件长度
        if (position + OF_DATA >= fileSize){
            return null;
        }
        // [Size] [Checksum] [Data]
        // 读取Size
        ByteBuffer temp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(temp);
        } catch (IOException e) {
            e.printStackTrace();
            Panic.panic(e);
        }

        int size = Parser.parseInt(temp.array());
        // 当前位置 +  data大小+ 数据偏移  > 文件长度，--> 超出文件位置
        if(position + size + OF_DATA > fileSize){
            return null;
        }

        // 读取checksum+data
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        byte[] log = buf.array();

        // 校验checkSum
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checkSum1 != checkSum2){
            return null;
        }

        position += log.length;
        return log;
    }

    /**
     * 倒回
     */
    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            raf.close();
            fc.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }
}

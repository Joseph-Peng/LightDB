package com.pjh.mydb.common;

/**
 * @author Joseph Peng
 * @date 2022/7/27 22:50
 */
public class Error {

    // TM
    public static final Exception BadTIDFileException = new RuntimeException("TID 文件异常！");

    // common
    public static final Exception FileExistsException = new RuntimeException("TID 文件已存在！");
    public static final Exception FileCannotRWException = new RuntimeException("TID 文件无法读写！");
    public static final Exception FileNotExistsException = new RuntimeException("TID 文件不存在!");
    public static final Exception CacheFullException  = new RuntimeException("缓存已满！");
}

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

    // DM
    public static final Exception MemTooSmallException   = new RuntimeException("缓存初始设置太小！");
    public static final Exception BadLogFileException = new RuntimeException("日志文件损坏！");
    public static final Exception DataTooLargeException = new RuntimeException("OOM，数据太大！");
    public static final Exception DatabaseBusyException = new RuntimeException("数据库繁忙！");

    // vm
    public static final Exception DeadlockException = new RuntimeException("Deadlock!");
    public static final Exception ConcurrentUpdateException = new RuntimeException("Concurrent update issue!");
    public static final Exception NullEntryException = new RuntimeException("Null entry!");

    // parser
    public static final Exception InvalidCommandException = new RuntimeException("Invalid command!");
    public static final Exception TableNoIndexException = new RuntimeException("Table has no index!");

    // tbm
    public static final Exception InvalidFieldException = new RuntimeException("Invalid field type!");
    public static final Exception FieldNotFoundException = new RuntimeException("Field not found!");
    public static final Exception FieldNotIndexedException = new RuntimeException("Field not indexed!");
    public static final Exception InvalidLogOpException = new RuntimeException("Invalid logic operation!");
    public static final Exception InvalidValuesException = new RuntimeException("Invalid values!");
    public static final Exception DuplicatedTableException = new RuntimeException("Duplicated table!");
    public static final Exception TableNotFoundException = new RuntimeException("Table not found!");

    // transport
    public static final Exception InvalidPkgDataException = new RuntimeException("Invalid package data!");

    // server
    public static final Exception NestedTransactionException = new RuntimeException("Nested transaction not supported!");
    public static final Exception NoTransactionException = new RuntimeException("Not in transaction!");

    // launcher
    public static final Exception InvalidMemException = new RuntimeException("Invalid memory!");


}

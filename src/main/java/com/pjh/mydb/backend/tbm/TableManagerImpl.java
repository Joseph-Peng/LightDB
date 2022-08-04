package com.pjh.mydb.backend.tbm;

import com.pjh.mydb.common.Error;
import com.pjh.mydb.backend.dm.DataManager;
import com.pjh.mydb.backend.parser.statement.*;
import com.pjh.mydb.backend.utils.Parser;
import com.pjh.mydb.backend.vm.VersionManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * TBM本身的模型如下:
 * 	[TBM] -> [Booter] -> [Table1] -> [Table2] -> [Table3] ...
 * 	TBM将它管理的所有的表, 以链表的结构组织起来.
 * 	并利用Booter, 存储了第一张表的UUID.
 *
 * @author Joseph Peng
 * @date 2022/8/4 12:52
 */
public class TableManagerImpl implements TableManager{
    VersionManager vm;
    DataManager dm;

    private Booter booter;
    private Map<String, Table> tableCache;            // 表缓存
    private Map<Long, List<Table>> tidTableCache;     // tid 创建了哪些表
    private Lock lock;

    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.tidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    private void loadTables() {
        long uid = firstTableUid();
        while(uid != 0) {
            Table tb = Table.loadTable(this, uid);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);
        }
    }

    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead ? 1 : 0;
        res.tid = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }

    @Override
    public byte[] commit(long tid) throws Exception {
        vm.commit(tid);
        return "commit".getBytes();
    }

    @Override
    public byte[] abort(long tid) {
        vm.abort(tid);
        return "abort".getBytes();
    }

    @Override
    public byte[] show(long tid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for(Table tb : tableCache.values()){
                sb.append(tb.toString()).append("\n");
            }
            List<Table> t = tidTableCache.get(tid);
            if (t == null){
                return "\n".getBytes();
            }
            for (Table tb : t) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        }finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] create(long tid, Create create) throws Exception {
        lock.lock();
        try {
            if(tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }
            Table table = Table.createTable(this, firstTableUid(), tid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            if(!tidTableCache.containsKey(tid)) {
                tidTableCache.put(tid, new ArrayList<>());
            }
            tidTableCache.get(tid).add(table);
            return ("create " + create.tableName).getBytes();
        }finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] insert(long tid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if (table == null){
            throw Error.TableNotFoundException;
        }
        table.insert(tid, insert);
        return "insert".getBytes();
    }

    @Override
    public byte[] read(long tid, Select read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        return table.read(tid, read).getBytes();
    }

    @Override
    public byte[] update(long tid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.update(tid, update);
        return ("update " + count).getBytes();
    }

    @Override
    public byte[] delete(long tid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.delete(tid, delete);
        return ("delete " + count).getBytes();
    }
}

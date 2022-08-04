package com.pjh.mydb.server.tbm;

import com.pjh.mydb.server.dm.DataManager;
import com.pjh.mydb.server.parser.statement.*;
import com.pjh.mydb.server.utils.Parser;
import com.pjh.mydb.server.vm.VersionManager;

public interface TableManager {

    BeginRes begin(Begin begin);
    byte[] commit(long tid) throws Exception;
    byte[] abort(long tid);

    byte[] show(long tid);
    byte[] create(long tid, Create create) throws Exception;

    byte[] insert(long tid, Insert insert) throws Exception;
    byte[] read(long tid, Select select) throws Exception;
    byte[] update(long tid, Update update) throws Exception;
    byte[] delete(long tid, Delete delete) throws Exception;

    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}

package com.pjh.mydb.backend;

import com.pjh.mydb.backend.dm.DataManager;
import com.pjh.mydb.backend.server.Server;
import com.pjh.mydb.backend.tbm.TableManager;
import com.pjh.mydb.backend.tm.TransactionManager;
import com.pjh.mydb.backend.utils.Panic;
import com.pjh.mydb.backend.vm.VersionManager;
import com.pjh.mydb.backend.vm.VersionManagerImpl;
import com.pjh.mydb.common.Error;
import org.apache.commons.cli.*;

/**
 *
 * 服务器的启动入口。这个类解析了命令行参数。
 * 很重要的参数就是 -open 或者 -create。Launcher
 * 根据两个参数，来决定是创建数据库文件，还是启动一个已有的数据库。
 *
 * @author Joseph Peng
 * @date 2022/8/4 23:32
 */
public class Launcher {
    public static final int port = 8888;

    public static final long DEFAULT_MEM = (1<<20) * 64;
    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options,args);

        if(cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }

        if(cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFAULT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFAULT_MEM;
        }

        if(memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }

        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch(unit) {
            case "KB":
                return memNum*KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFAULT_MEM;
    }


}

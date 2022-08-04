package com.pjh.mydb.backend.tbm;

import com.pjh.mydb.common.Error;
import com.pjh.mydb.backend.utils.Panic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * MYDB 使用 Booter 类和 bt 文件，来管理 MYDB 的启动信息
 * 记录第一个表的uid
 * @author Joseph Peng
 * @date 2022/8/4 14:58
 */
public class Booter {

    public static final String BOOTER_SUFFIX = ".bt";
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    String path;
    File file;

    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    public static Booter create(String path) {
        removeBadTmp(path);
        File f = new File(path + BOOTER_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    public static Booter open(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    private static void removeBadTmp(String path) {
        new File(path+BOOTER_TMP_SUFFIX).delete();
    }

    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf;
    }

    /**
     *     update 在修改 bt 文件内容时，没有直接对 bt 文件进行修改，
     *     而是首先将内容写入一个 bt_tmp 文件中，随后将这个文件重命名为 bt 文件。
     *     以期通过操作系统重命名文件的原子性，来保证操作的原子性。
     */
    public void update(byte[] data){
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmp.createNewFile();
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        try(FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch(IOException e) {
            Panic.panic(e);
        }

        try {
            Files.move(tmp.toPath(), new File(path+BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch(IOException e) {
            Panic.panic(e);
        }
        file = new File(path+BOOTER_SUFFIX);
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
    }


}

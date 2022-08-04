package com.pjh.mydb.backend.server;

import com.pjh.mydb.backend.parser.statement.*;
import com.pjh.mydb.common.Error;
import com.pjh.mydb.backend.parser.Parser;
import com.pjh.mydb.backend.tbm.BeginRes;
import com.pjh.mydb.backend.tbm.TableManager;

/**
 * @author Joseph Peng
 * @date 2022/8/4 21:54
 */
public class Executor {

    private long tid;
    TableManager tbm;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.tid = 0;
    }

    public void close() {
        if(tid != 0) {
            System.out.println("Abnormal Abort: " + tid);
            tbm.abort(tid);
        }
    }

    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        Object stat = Parser.Parse(sql);
        if(Begin.class.isInstance(stat)) {
            if(tid != 0) {
                throw Error.NestedTransactionException;
            }
            BeginRes r = tbm.begin((Begin)stat);
            tid = r.tid;
            return r.result;
        }else if(Commit.class.isInstance(stat)) {
            if(tid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.commit(tid);
            tid = 0;
            return res;
        } else if(Abort.class.isInstance(stat)) {
            if(tid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.abort(tid);
            tid = 0;
            return res;
        } else {
            return execute2(stat);
        }
    }

    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if(tid == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            tid = r.tid;
        }
        try {
            byte[] res = null;
            if(Show.class.isInstance(stat)) {
                res = tbm.show(tid);
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(tid, (Create)stat);
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(tid, (Select)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(tid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(tid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(tid, (Update)stat);
            }
            return res;
        } catch(Exception e1) {
            e = e1;
            throw e;
        } finally {
            if(tmpTransaction) {
                if(e != null) {
                    tbm.abort(tid);
                } else {
                    tbm.commit(tid);
                }
                tid = 0;
            }
        }
    }
}

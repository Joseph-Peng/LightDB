package com.pjh.mydb.backend.im;

import com.pjh.mydb.backend.common.SubArray;
import com.pjh.mydb.backend.dm.DataManager;
import com.pjh.mydb.backend.dm.dataitem.DataItem;
import com.pjh.mydb.backend.utils.Parser;
import com.pjh.mydb.backend.tm.TransactionManagerImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * IM 对上层模块主要提供两种能力：插入索引和搜索节点。
 * @author Joseph Peng
 * @date 2022/8/3 11:40
 */
public class BPlusTree {

    DataManager dm;
    long bootUid;
    /**
     * B+ 树在插入删除时，会动态调整，根节点不是固定节点
     * bootDataItem 存储了根节点的 UID
     */
    DataItem bootDataItem;
    Lock bootLock;

    /**
     * 创建一棵B+树, 并返回其bootUID.
     * @param dm
     * @return
     * @throws Exception
     */
    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_TID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_TID, Parser.long2Byte(rootUid));
    }

    /**
     * 通过bootUid读取一课B+树, 并返回它.
     * @param bootUid
     * @param dm
     * @return
     * @throws Exception
     */
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    /**
     * 获取根节点的UID
     * @return
     */
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 更新该树的根节点
     * @param left
     * @param right
     * @param rightKey
     * @throws Exception
     */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_TID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_TID);
        }finally {
            bootLock.unlock();
        }
    }

    /**
     * 根据key, 在nodeUID代表节点的子树中搜索, 直到找到其对应的叶节点地址.
     * @param nodeUid
     * @param key
     * @return
     * @throws Exception
     */
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if (isLeaf){
            return nodeUid;
        }else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    /**
     * 寻找对应key的uid, 如果找不到, 则返回sibling uid兄弟节点的
     * @param nodeUid
     * @param key
     * @return
     * @throws Exception
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while (true){
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            if (res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }

    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while (true){
            Node leaf = Node.loadNode(this, leafUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    class InsertRes {
        long newNode, newKey;
    }

    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if(res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if(isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        }else{
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if(ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    /**
     * 函数从node开始, 不断的向右试探兄弟节点, 直到找到一个节点, 能够插入进对应的值
     * @param nodeUid
     * @param uid
     * @param key
     * @return
     * @throws Exception
     */
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}

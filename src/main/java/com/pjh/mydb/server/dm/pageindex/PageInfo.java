package com.pjh.mydb.server.dm.pageindex;

/**
 * @author Joseph Peng
 * @date 2022/8/1 17:18
 */
public class PageInfo {

    public int pageNo;
    public int freeSpace;

    public PageInfo(int pageNo, int freeSpace) {
        this.pageNo = pageNo;
        this.freeSpace = freeSpace;
    }
}

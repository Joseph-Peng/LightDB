package com.pjh.mydb.server.utils;

/**
 * @author Joseph Peng
 * @date 2022/8/2 10:04
 */
public class Types {

    /**
     * 高32位存储页号
     * 低32位存储偏移
     * @param pageNo
     * @param offset
     * @return
     */
    public static long addressToUid(int pageNo, short offset) {
        long u0 = (long)pageNo;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }
}

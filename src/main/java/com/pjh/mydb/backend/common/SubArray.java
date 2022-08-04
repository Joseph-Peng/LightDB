package com.pjh.mydb.backend.common;

/**
 * @author Joseph Peng
 * @date 2022/7/28 19:15
 *
 * 简陋的共享内存数组的实现
 * start 和 end 规定了数组的可使用范围
 */
public class SubArray {

    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}

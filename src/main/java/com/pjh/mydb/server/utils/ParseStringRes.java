package com.pjh.mydb.server.utils;

/**
 * @author Joseph Peng
 * @date 2022/8/4 13:09
 */
public class ParseStringRes {

    // 字段值
    public String str;
    // 当前字段长度
    public int next;

    public ParseStringRes(String str, int next) {
        this.str = str;
        this.next = next;
    }
}

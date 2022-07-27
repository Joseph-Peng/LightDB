package com.pjh.mydb.common;

import java.nio.ByteBuffer;

/**
 * @author Joseph Peng
 * @date 2022/7/27 18:12
 */
public class Parser {

    public static long parseLong(byte[] buffer){
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, 8);
        return byteBuffer.getLong();
    }

    public static byte[] long2byte(long value){
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }
}

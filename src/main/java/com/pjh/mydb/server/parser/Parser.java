package com.pjh.mydb.server.parser;

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

    public static short parseShort(byte[] buf){
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, 2);
        return byteBuffer.getShort();
    }

    public static byte[] long2byte(long value){
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static byte[] short2byte(short value){
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }
}

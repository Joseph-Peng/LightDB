package com.pjh.mydb.backend.utils;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

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

    public static byte[] long2Byte(long value){
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static byte[] short2Byte(short value){
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static int parseInt(byte[] buf) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, 4);
        return byteBuffer.getInt();
    }

    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    /**
     * [StringLength][StringData]
     * 返回数据的长度和数据
     * @param raw
     * @return
     */
    public static ParseStringRes parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4+length));
        return new ParseStringRes(str, length+4);
    }

    public static byte[] string2Byte(String str) {
        byte[] l = int2Byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for(byte b : key.getBytes()) {
            res = res * seed + (long)b;
        }
        return res;
    }
}

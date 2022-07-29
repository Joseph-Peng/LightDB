package com.pjh.mydb.server.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * @author Joseph Peng
 * @date 2022/7/29 15:31
 */
public class RandomUtil {

    public static byte[] randomBytes(int length){
        Random r = new SecureRandom();
        byte[] buf = new byte[length];
        r.nextBytes(buf);
        return buf;
    }
}

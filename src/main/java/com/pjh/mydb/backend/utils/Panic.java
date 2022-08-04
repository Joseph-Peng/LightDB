package com.pjh.mydb.backend.utils;

/**
 * @author Joseph Peng
 * @date 2022/7/27 22:48
 */
public class Panic {

    public static void panic(Exception e){
        e.printStackTrace();
        System.exit(1);
    }
}

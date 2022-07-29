package com.pjh.mydb.server.dm.page;

import com.pjh.mydb.server.dm.pageCache.PageCache;
import com.pjh.mydb.server.utils.RandomUtil;

import java.util.Arrays;

/**
 * DB文件第一页，用于启动检查
 * 在每次数据库启动时，会生成一串随机字节，存储在 100 ~ 107 字节。
 * 在数据库正常关闭时，会将这串字节，拷贝到第一页的 108 ~ 115 字节。
 * 数据库在每次启动时，就会检查第一页两处的字节是否相同
 *
 * @author Joseph Peng
 * @date 2022/7/29 15:15
 */
public class PageOne {
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    // 数据库启动时，会生成一串随机字节，存储在 100 ~ 107 字节
    private static void setVcOpen(byte[] raw) {
        /** 把一个数组中某一段字节数据放到另一个数组中
         * src:源数组;
         * srcPos:源数组要复制的起始位置;
         * dest:目的数组;
         * destPos:目的数组放置的起始位置;
         * length:复制的长度.
         */
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    // 关闭时 拷贝到第一页的 108 ~ 115 字节
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page page) {
        return checkVc(page.getData());
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC), Arrays.copyOfRange(raw, OF_VC + LEN_VC,OF_VC + LEN_VC + LEN_VC));
    }
}

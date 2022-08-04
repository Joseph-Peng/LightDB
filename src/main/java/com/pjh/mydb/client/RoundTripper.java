package com.pjh.mydb.client;

import com.pjh.mydb.transport.Packager;
import com.pjh.mydb.transport.Package;

/**
 * @author Joseph Peng
 * @date 2022/8/4 21:54
 */
public class RoundTripper {

    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }

}

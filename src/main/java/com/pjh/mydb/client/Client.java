package com.pjh.mydb.client;

import com.pjh.mydb.transport.Package;
import com.pjh.mydb.transport.Packager;

/**
 * @author Joseph Peng
 * @date 2022/8/4 21:53
 */
public class Client {
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = rt.roundTrip(pkg);
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }
}

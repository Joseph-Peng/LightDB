package com.pjh.mydb.transport;

import java.io.IOException;

/**
 * @author Joseph Peng
 * @date 2022/8/4 21:50
 */
public class Packager {

    private Transporter transporter;
    private Encoder encoder;

    public Packager(Transporter transporter, Encoder encoder){
        this.transporter = transporter;
        this.encoder = encoder;
    }

    public void send(Package pkg) throws Exception {
        byte[] data = encoder.encode(pkg);
        transporter.send(data);
    }

    public Package receive() throws Exception{
        byte[] data = transporter.receive();
        return encoder.decode(data);
    }

    public void close() throws IOException {
        transporter.close();
    }
}

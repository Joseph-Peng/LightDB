package com.pjh.mydb.client;

import com.pjh.mydb.transport.Encoder;
import com.pjh.mydb.transport.Packager;
import com.pjh.mydb.transport.Transporter;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 *
 *
 * @author Joseph Peng
 * @date 2022/8/4 21:53
 */
public class Launcher {

    public static void main(String[] args) throws UnknownHostException, IOException {
        Socket socket = new Socket("127.0.0.1", 8888);
        Encoder e = new Encoder();
        Transporter t = new Transporter(socket);
        Packager packager = new Packager(t, e);

        Client client = new Client(packager);
        Shell shell = new Shell(client);

        shell.run();
    }
}

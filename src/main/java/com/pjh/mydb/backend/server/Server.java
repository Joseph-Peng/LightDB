package com.pjh.mydb.backend.server;

import com.pjh.mydb.backend.tbm.TableManager;
import com.pjh.mydb.transport.Encoder;
import com.pjh.mydb.transport.Package;
import com.pjh.mydb.transport.Packager;
import com.pjh.mydb.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Joseph Peng
 * @date 2022/8/4 21:54
 */
public class Server {

    private int port;
    TableManager tbm;

    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    public void start() {
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(port);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port: " + port);

        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                5, 10, 1L,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(100),
                new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while (true){
                Socket socket = ss.accept();
                Runnable worker = new HandleSocket(socket, tbm);
                threadPool.execute(worker);
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                ss.close();
            }catch (IOException e){

            }
        }

    }

}

class HandleSocket implements Runnable {

    private Socket socket;
    private TableManager tbm;

    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    @Override
    public void run() {
        InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();
        System.out.println("Establish connection: " + address.getAddress().getHostAddress()+":"+address.getPort());
        Packager packager = null;
        try {
            Transporter transporter = new Transporter(socket);
            Encoder encoder = new Encoder();
            packager = new Packager(transporter, encoder);
        }catch (Exception e){
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return;
        }

        Executor executor = new Executor(tbm);
        while (true){
            Package pkg = null;
            try {
                pkg = packager.receive();
            }catch (Exception e){
                e.printStackTrace();
                break;
            }
            byte[] sql = pkg.getData();
            byte[] result = null;
            Exception err = null;
            try {
                result = executor.execute(sql);
            }catch (Exception e){
                err = e;
                e.printStackTrace();
            }

            pkg = new Package(result, err);
            try {
                packager.send(pkg);
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }

    }
}

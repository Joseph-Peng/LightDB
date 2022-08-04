package com.pjh.mydb.client;

import java.util.Scanner;

/**
 * @author Joseph Peng
 * @date 2022/8/4 21:54
 */
public class Shell {
    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    public void run() {
        Scanner sc = new Scanner(System.in);
        try {
            while(true) {
                System.out.print(":> ");
                String statStr = sc.nextLine();
                if("exit".equals(statStr) || "quit".equals(statStr)) {
                    break;
                }
                try {
                    byte[] res = client.execute(statStr.getBytes());
                    System.out.println(new String(res));
                } catch(Exception e) {
                    System.out.println(e.getMessage());
                }

            }
        } finally {
            sc.close();
            client.close();
        }
    }
}
//mvn exec:java -Dexec.mainClass="com.pjh.mydb.backend.Launcher" -Dexec.args="-create F:/TestDB/SimpleDB"
//mvn exec:java -Dexec.mainClass="com.pjh.mydb.backend.Launcher" -Dexec.args="-open F:/TestDB/SimpleDB"
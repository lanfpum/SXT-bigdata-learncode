package lxpsee.top.oldlearn.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/14 08:24.
 * <p>
 * 给远端发送sock信息工具类
 */
public class ShowDetailUtil {
    public static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static String getPID() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        return name.split("@")[0];
    }

    public static String getTID() {
        return Thread.currentThread().getName();
    }

    public static String getOID(Object o) {
        return o.getClass().getSimpleName() + "@" + o.hashCode();
    }

    public static String getINFO(Object o, String msg) {
        return "HostName:" + getHostName() + ",PID:" + getPID() + ",TID:" + getTID()
                + "，OID:" + getOID(o) + "msg:" + msg;
    }

    public static void sendTpClient(Object o, String msg, String host, int port) {
        try {
            String info = getINFO(o, msg);
            Socket socket = new Socket(host, port);
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write((info + "\r\n").getBytes());
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

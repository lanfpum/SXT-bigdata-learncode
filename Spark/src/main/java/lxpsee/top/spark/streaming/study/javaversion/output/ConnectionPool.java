package lxpsee.top.spark.streaming.study.javaversion.output;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/5/15 19:43.
 * <p>
 * 简易版的连接池
 */
public class ConnectionPool {
    // 静态的Connection队列
    private static LinkedList<Connection> connectionQueue;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接
     */
    public synchronized static Connection getConnection() {
        try {
            if (null == connectionQueue) {
                connectionQueue = new LinkedList<Connection>();

                for (int i = 0; i < 10; i++) {
                    Connection connection = DriverManager.getConnection(
                            "jdbc:mysql://localhost:3306/lanp_test_db",
                            "root",
                            "123");
                    connectionQueue.push(connection);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return connectionQueue.poll();
    }

    // 返回连接
    public static void returnCOnmnection(Connection connection) {
        connectionQueue.push(connection);
    }

}

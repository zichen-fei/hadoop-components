package cn.components.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class Phoenix {

    private static final Logger logger = LoggerFactory.getLogger(Phoenix.class);

    private Connection conn;
    private Statement sm;

    // 用于初始化连接
    public void parpare() {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection("jdbc:phoenix:10.10.21.13:2181");
            sm = conn.createStatement();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 查询数据
    public void selectAll() {
        try {
            ResultSet rs = sm.executeQuery("select * from  \"ev_data\" limit 1");
            ResultSet rs1 = sm.executeQuery("select * from  \"ev_data\" limit 1");
            ResultSet rs2 = sm.executeQuery("select * from  \"ev_data\" limit 1");

            while (rs.next()) {
                logger.info("{}, {}, {}", rs.getString(1), rs.getString(2), rs.getString(3));
            }
            while (rs1.next()) {
                logger.info("{}, {}", rs1.getString(1), rs1.getString(2));
            }
            while (rs2.next()) {
                logger.info("{}, {}, {}", rs2.getString(1), rs2.getString(2), rs2.getString(3));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 用于关闭资源
    public void end() {

        try {
            sm.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Phoenix p = new Phoenix();
        p.parpare();
        p.selectAll();

        p.end();
    }

}

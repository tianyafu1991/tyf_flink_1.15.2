package com.tyf.flink.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class MySQLUtils {

    public static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";
    public static final String URL = "jdbc:mysql://sdw2:3306/tyf_flink?autoReconnect=true&useSSL=false&useUnicode=true&characterEncoding=UTF-8";
    public static final String USERNAME = "tyf";
    public static final String PASSWORD = "tyf";


    public static Connection getConnection() throws Exception {
        Class.forName(MYSQL_DRIVER_NAME);
        return DriverManager.getConnection(URL, USERNAME, PASSWORD);
    }

    public static void close(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                closeable = null;
            }
        }
    }
}

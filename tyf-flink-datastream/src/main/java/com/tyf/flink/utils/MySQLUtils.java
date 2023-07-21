package com.tyf.flink.utils;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;

public class MySQLUtils {


    public static Connection getConnection() throws Exception{
        Class.forName("com.mysql.jdbc.Driver");
        return DriverManager.getConnection("jdbc:mysql://sdw2:3306/tyf_flink?autoReconnect=true&useSSL=false&useUnicode=true&characterEncoding=UTF-8","tyf","tyf");
    }

    public static void close(AutoCloseable closeable){
        if (closeable != null){
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                closeable = null;
            }
        }
    }
}

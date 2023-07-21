package com.tyf.flink.source;

import com.tyf.flink.bean.Student;
import com.tyf.flink.utils.MySQLUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class TyfMySQLSource extends RichSourceFunction<Student> {


    Connection connection;

    PreparedStatement stat;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MySQLUtils.getConnection();
        stat = connection.prepareStatement("select * from student");
    }

    @Override
    public void close() throws Exception {
        MySQLUtils.close(stat);
        MySQLUtils.close(connection);
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet rs = stat.executeQuery();
        while(rs.next()) {
            Student student = new Student(rs.getInt("id"), rs.getString("name"), rs.getInt("age"));
            ctx.collect(student);
        }
    }

    @Override
    public void cancel() {

    }
}

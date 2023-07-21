package com.tyf.flink.source;

import com.tyf.flink.bean.Access;
import com.tyf.flink.bean.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceApp {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8082);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

//        DataStreamSource<String> source = env.readTextFile("data/wc.data");
//        DataStreamSource<Access> source = env.addSource(new TyfAccessSource());
//        DataStreamSource<Access> source = env.addSource(new TyfParallelAccessSource()).setParallelism(4);
//        DataStreamSource<Access> source = env.addSource(new TyfRichParallelAccessSource()).setParallelism(4);
        DataStreamSource<Student> source = env.addSource(new TyfMySQLSource());

        System.out.println(source.getParallelism());
        source.print();

        env.execute("SourceApp");
    }
}

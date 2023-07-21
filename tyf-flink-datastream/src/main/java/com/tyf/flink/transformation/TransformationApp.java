package com.tyf.flink.transformation;

import com.tyf.flink.bean.Access;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformationApp {


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8082);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("data/access.log");
        source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] splits = value.split(",");
                return new Access(Long.valueOf(splits[0].trim()),splits[1].trim(),Double.valueOf(splits[2].trim()));
            }
        }).keyBy(x -> x.getName()).sum("traffic").print();


        env.execute("TransformationApp");
    }
}

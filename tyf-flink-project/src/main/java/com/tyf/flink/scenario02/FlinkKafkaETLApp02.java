package com.tyf.flink.scenario02;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink异步IO
 */
public class FlinkKafkaETLApp02 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream("sdw2", 9527);

//        AsyncDataStream.orderedWait(stream,new MySQLAsyncFunction(3),)


        env.execute("FlinkKafkaETLApp02");
    }
}

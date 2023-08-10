package com.tyf.flink.scenario02;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * Flink异步IO
 */
public class FlinkKafkaETLApp02 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream("sdw2", 9527);
        int capacity = 20;
        SingleOutputStreamOperator<Tuple2<String, String>> result = AsyncDataStream.orderedWait(
                stream
                , new MySQLAsyncFunction(capacity)
                , 3000
                , TimeUnit.MILLISECONDS
                , capacity
        );

        result.print();


        env.execute("FlinkKafkaETLApp02");
    }
}

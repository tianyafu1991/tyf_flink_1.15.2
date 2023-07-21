package com.tyf.flink.basic;

import com.tyf.flink.basic.function.TyfFlatMapFunction;
import com.tyf.flink.basic.function.TyfMapFunction;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWCApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStreamSource<String> source = env.readTextFile("data/wc.data");
        source.flatMap(new TyfFlatMapFunction()).map(new TyfMapFunction())
                .keyBy(x->x.f0)
                .sum(1)
                .print();

        env.execute("StreamWCApp");


    }
}

package com.tyf.flink.basic;

import com.tyf.flink.basic.function.TyfFlatMapFunction;
import com.tyf.flink.basic.function.TyfMapFunction;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWCApp02 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool tool = ParameterTool.fromArgs(args);
        // --host sdw2 --port 9528
        String host = tool.get("host", "sdw2");
        int port = tool.getInt("port", 9527);

        DataStreamSource<String> source = env.socketTextStream(host, port);

        source.flatMap(new TyfFlatMapFunction()).map(new TyfMapFunction())
                .keyBy(x->x.f0)
                .sum(1)
                .print();

        env.execute("StreamWCApp02");


    }
}

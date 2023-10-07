package com.tyf.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/fault-tolerance/checkpointing/
 */
public class CheckpointApp {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 下次启动后 就会去这个路径下找到上次存储的状态进行恢复
        conf.setString("execution.savepoint.path","file:///D:\\personal\\code\\flink\\tianyafu_flink\\tyf_flink\\chk\\8adeb2f9d3b693f4ed3bb6aebca6a73b\\chk-11");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:\\personal\\code\\flink\\tianyafu_flink\\tyf_flink\\chk");
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5));

        env.socketTextStream("sdw2",9527)
                .map(new TyfMapFunction())
                .print();


        env.execute("CustomStateApp01");
    }

}

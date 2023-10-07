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

public class CustomStateApp01 {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 下次启动后 就会去这个路径下找到上次存储的状态进行恢复
        conf.setString("execution.savepoint.path","file:///D:\\personal\\code\\flink\\tianyafu_flink\\tyf_flink\\chk\\8adeb2f9d3b693f4ed3bb6aebca6a73b\\chk-11");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:\\personal\\code\\flink\\tianyafu_flink\\tyf_flink\\chk");
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5));

        env.socketTextStream("sdw2",9527)
                .map(new TyfMapFunction())
                .print();


        env.execute("CustomStateApp01");
    }


    public static void test02(StreamExecutionEnvironment env){
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("chk");
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5));

        env.socketTextStream("sdw2",9527)
                .keyBy(x -> "0")
                .map(new RichMapFunction<String, String>() {
                    ListState<String> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor("list",String.class));
                    }

                    @Override
                    public String map(String value) throws Exception {
                        listState.add(value.toLowerCase(Locale.ROOT));
                        StringBuilder builder = new StringBuilder();
                        for (String s : listState.get()) {
                            builder.append(s);
                        }
                        return builder.toString();
                    }
                }).print();
    }


    public static void test01(StreamExecutionEnvironment env){
        env.socketTextStream("sdw2",9527).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toLowerCase(Locale.ROOT);
            }
        }).flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] splits = value.split(",");
                for (String split : splits) {
                    out.collect(Tuple2.of(split,1L));
                }
            }
        }).keyBy(x -> x.f0)
                .map(new MapFunction<Tuple2<String, Long>, Tuple2<String,Long>>() {
                    Map<String,Long> state = new HashMap<>();
                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                        String word = value.f0;
                        Long cnt = value.f1;
                        Long historyValue = state.getOrDefault(word, 0L);
                        System.out.println("进来的数据为:" + word + ",次数为:" + cnt);
                        historyValue += cnt;
                        state.put(word,historyValue);
                        return Tuple2.of(word,historyValue);
                    }
                }).print();
    }
}

package com.tyf.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 状态的Ttl代码示例
 */
public class StateTtlApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("sdw2", 9527)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] splits = value.toLowerCase(Locale.ROOT).split(",");
                        for (String split : splits) {
                            out.collect(Tuple2.of(split, 1L));
                        }
                    }
                })
                .keyBy(x -> x.f0)
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String,Long>>() {
                    private transient ValueState<Long> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("value", Long.class);
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10L))
                                .updateTtlOnCreateAndWrite()
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                                .build()
                                ;
                        stateDescriptor.enableTimeToLive(ttlConfig);

                        valueState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                        Long historyCnt = valueState.value();
                        if(null == historyCnt){
                            historyCnt = 0L;
                        }
                        Long current = historyCnt + value.f1;
                        valueState.update(current);
                        return Tuple2.of(value.f0,current);
                    }
                })
                .print();

        env.execute("ValueStateApp");
    }
}

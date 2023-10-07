package com.tyf.flink.state;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Locale;

/**
 * process：
 * 用来处理单条数据 类似于map
 *
 * 针对keyed stream 也可以用来处理状态
 *
 * 整合定时器来进行触发: 同一个key上可以注册多个定时器，时间到了会触发onTimer方法的执行
 * 既然我们可以通过process定义定时器的执行
 * 结合eventTime + waterMark + 不允许你使用window的时候 我们就可以使用process来完成窗口的统计分析
 *
 */
public class ProcessFunctionApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.execute("ProcessFunctionApp");
    }

    public static void test03(StreamExecutionEnvironment env){
        env.socketTextStream("sdw2",9527)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] splits = value.toLowerCase(Locale.ROOT).split(",");
                        for (String split : splits) {
                            out.collect(Tuple2.of(split.trim(),1L));
                        }
                    }
                })
                .keyBy(x -> x.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String,Long>>() {

                    private transient ValueState<Long> valueState = null;

                    FastDateFormat format = FastDateFormat.getInstance("HH:mm:ss");

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("valueState",Long.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        long currentTimeMillis = System.currentTimeMillis();
                        long triggerTime = currentTimeMillis + 15000; // 定义了一个定时时间，在15秒后触发
                        System.out.println(ctx.getCurrentKey() + "，注册了定时器: " + format.format(triggerTime));
                        ctx.timerService().registerProcessingTimeTimer(triggerTime);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        System.out.println(ctx.getCurrentKey() + "在 " + format.format(timestamp) + "触发了定时器的执行");
                    }
                }).print();
    }

    /**
     * 针对keyed stream 也可以用来处理状态
     * @param env
     */
    public static void test02(StreamExecutionEnvironment env){
        env.socketTextStream("sdw2",9527)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] splits = value.toLowerCase(Locale.ROOT).split(",");
                        for (String split : splits) {
                            out.collect(Tuple2.of(split.trim(),1L));
                        }
                    }
                })
                .keyBy(x -> x.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String,Long>>() {

                    private transient ValueState<Long> valueState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("valueState",Long.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        Long history = valueState.value();
                        if(null == history){
                            history = 0L;
                        }
                        history += value.f1;
                        valueState.update(history);
                        out.collect(Tuple2.of(value.f0,history));
                    }
                }).print();
    }


    /**
     * 用来处理单条数据 类似于map
     * @param env
     */
    public static void test01(StreamExecutionEnvironment env){
        /**
         * 接入的数据为json数据:
         * {"id":1,"name":"zs","age":30}
         */

        DataStreamSource<String> lines = env.socketTextStream("sdw2", 9527);

        lines.process(new ProcessFunction<String, User>() {
            @Override
            public void processElement(String value, Context ctx, Collector<User> out) throws Exception {
                try {
                    out.collect(JSON.parseObject(value,User.class));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).print();
    }
}

package com.tyf.flink.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowApp {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

//        countWindows(env);
//        countKeyByWindows(env);
//        tumblingWindow(env);
//        tumblingKeyByWindow(env);
//        slidingWindow(env);
//        slidingKeyByWindow(env);
//        sessionWindow(env);
        sessionKeyByWindow(env);
        env.execute("WindowApp");
    }
    /**
     * 带Key By 的 session window
     * 一直有数据一直不触发  直到5秒都没有数据  表示session不活跃了 就触发window计算
     *
     * @param env
     */
    public static void sessionKeyByWindow(StreamExecutionEnvironment env) {
        env.socketTextStream("sdw2", 9527)
                .map(x -> {
                    String[] splits = x.split(",");
                    return new Tuple2(splits[0].trim(), Integer.valueOf(splits[1].trim()));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(1)
                .print()
        ;
    }

    /**
     * session window
     * 一直有数据一直不触发  直到5秒都没有数据  表示session不活跃了 就触发window计算
     *
     * @param env
     */
    public static void sessionWindow(StreamExecutionEnvironment env) {
        env.socketTextStream("sdw2", 9527)
                .map(x -> Integer.parseInt(x.trim()))
                .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(0)
                .print()
        ;
    }


    /**
     * 带 Key By的滑动窗口
     * 每10秒1个窗口 每5秒滑动1次
     *
     * @param env
     */
    public static void slidingKeyByWindow(StreamExecutionEnvironment env) {
        env.socketTextStream("sdw2", 9527)
                .map(x -> {
                    String[] splits = x.split(",");
                    return new Tuple2(splits[0].trim(), Integer.valueOf(splits[1].trim()));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(1)
                .print();
    }

    /**
     * 滑动窗口
     * 每10秒1个窗口 每5秒滑动1次
     *
     * @param env
     */
    public static void slidingWindow(StreamExecutionEnvironment env) {
        env.socketTextStream("sdw2", 9527)
                .map(x -> Integer.parseInt(x.trim()))
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(0)
                .print()
        ;
    }


    /**
     * 带Key By 的 处理时间 的window
     * 每5秒1个窗口
     *
     * @param env
     */
    public static void tumblingKeyByWindow(StreamExecutionEnvironment env) {
        env.socketTextStream("sdw2", 9527)
                .map(x -> {
                    String[] splits = x.split(",");
                    return new Tuple2(splits[0].trim(), Integer.valueOf(splits[1].trim()));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();
    }


    /**
     * 处理时间 的window
     * 每5秒1个窗口
     *
     * @param env
     */
    public static void tumblingWindow(StreamExecutionEnvironment env) {
        env.socketTextStream("sdw2", 9527)
                .map(x -> Integer.parseInt(x.trim()))
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(0)
                .print()
        ;
    }

    /**
     * nc端输入数据，每一行就一个数字，window为count的 元素个数为5 求和
     * 1
     * 2
     * 3
     * 4
     * 5
     *
     * @param env
     */
    public static void countWindows(StreamExecutionEnvironment env) {
        env.socketTextStream("sdw2", 9527)
                .map(x -> Integer.parseInt(x.trim()))
                .countWindowAll(5)
                .sum(0)
                .print();
    }

    /**
     * 因为是keyBy的 所以只有相同的key的元素个数达到指定数量 才会触发执行
     *
     * @param env
     */
    public static void countKeyByWindows(StreamExecutionEnvironment env) {
        env.socketTextStream("sdw2", 9527)
                .map(x -> {
                    String[] splits = x.split(",");
                    return new Tuple2(splits[0].trim(), Integer.valueOf(splits[1].trim()));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0)
                .countWindow(5)
                .sum(1)
                .print();
    }
}

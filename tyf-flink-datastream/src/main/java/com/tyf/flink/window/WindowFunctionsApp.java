package com.tyf.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class WindowFunctionsApp {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);


        test03(env);

        env.execute("WindowFunctionsApp");
    }


    public static void test03(StreamExecutionEnvironment env) {
        env.socketTextStream("sdw2", 9527)
                .map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) throws Exception {
                        return Integer.valueOf(value.trim());
                    }
                })
        .countWindowAll(5)
        .process(new ProcessAllWindowFunction<Integer, Integer, GlobalWindow>() {
            @Override
            public void process(Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
                List<Integer> result = new ArrayList<>();
                for (Integer element : elements) {
                    result.add(element);
                }
                result.sort(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o1 - o2;
                    }
                });
                for (Integer value : result) {
                    out.collect(value);
                }
            }
        })
        .print().setParallelism(1)
        ;
    }


    /**
     * 求平均数
     * a,100
     * a,2
     *
     * 51.0
     * @param env
     */
    public static void test02(StreamExecutionEnvironment env) {
        env.socketTextStream("sdw2", 9527)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] splits = value.split(",");
                        return Tuple2.of(splits[0].trim(),Long.valueOf(splits[1].trim()));
                    }
                })
                .keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new TyfAvgFunction())
                .print()
                ;
    }

    /**
     * 一行一个数字
     * 要求：让你们一定要使用KeyBy
     * 5秒一个滚动窗口 按照相同的key求和 不允许使用sum
     *
     * @param env
     */
    public static void test01(StreamExecutionEnvironment env) {
        env.socketTextStream("sdw2", 9527)
                .map(x -> Tuple2.of(1, Integer.parseInt(x.trim())))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> x, Tuple2<Integer, Integer> y) throws Exception {
                        System.out.println("执行reduce操作:" + x + " , " + y);
                        return Tuple2.of(x.f0, x.f1 + y.f1);
                    }
                }).print();

                /*.reduce((x, y) -> {
                    System.out.println("执行reduce操作:" + x + " , " + y);
                    return Tuple2.of(x.f0, x.f1 + y.f1);
                })
                .print()
                ;*/
    }
}

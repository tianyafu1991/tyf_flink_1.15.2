package com.tyf.flink.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowFunctionsApp {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);


        test01(env);

        env.execute("WindowFunctionsApp");
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

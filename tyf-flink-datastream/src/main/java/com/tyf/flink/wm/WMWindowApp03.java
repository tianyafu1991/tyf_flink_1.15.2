package com.tyf.flink.wm;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class WMWindowApp03 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("sdw2", 9527);
        // 事件时间,domain,traffic  开窗口  求窗口内每个domain出现的次数
        //1000,a,1
        //2000,a,1
        //3000,b,1
        //5000,c,1
        //4000,a,1
        //4200,a,1
        //8888,a,1
        //9999,b,1
        //10000,b,1
        //12000,b,1
        /**
         * 滚动窗口是5秒滚一次 解决乱序问题
         * 当forBoundedOutOfOrderness中容忍度为0时 窗口的开始时间和结束时间是:
         * [window_start,window_end)
         * [0000,5000)  等到事件时间大于等于5000时 会触发此窗口的执行 之后的4000和4200这2条数据会丢掉
         * [5000,10000)
         * 当 watermark >= window_end时  就会触发窗口的执行
         *
         * 当数据小延迟时(数据延迟时间不大)  用这种容忍度
         * 当forBoundedOutOfOrderness中容忍度为2秒时 窗口的开始时间和结束时间是:
         * [0000,5000)  但是要等到事件时间大于等于7000时 才会触发此窗口的执行 这就是容忍度为2秒的含义
         * [5000,10000)
         *
         * 当数据中延迟时(数据延迟时间不大不小) 用allowedLateness  这种方式与forBoundedOutOfOrderness的区别是触发的时机不一样
         * 假设当forBoundedOutOfOrderness中容忍度为0秒时  allowedLateness 为2秒
         * [0000,5000)  在事件时间大于等于5000时 触发窗口的计算 当延迟数据4000,a,1进来后 再次触发本窗口中key为a的数据的计算 同理 4200,a,1进来后 又触发了
         * [5000,10000)
         *
         *
         * 当数据大延迟时 用sideOutputLateData
         * 假设当forBoundedOutOfOrderness中容忍度为0秒时  且没有设置allowedLateness 此时
         * [0000,5000)  在事件时间大于等于5000时 触发窗口的计算 当延迟数据4000,a,1进来后 在测流中输出 同理4200,a,1进来后 也在测流中输出  即测流中的延迟数据是不进入主流中执行主流的逻辑
         * [5000,10000)
         * 测流输出可以将数据输出到数据库中 后面对主流的结果再进行补数操作修正结果
         *
         * 在实际生产中 以上3中方式同时使用 因为不确定到底延迟会有多大
         *
         */
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(event.split(",")[0].trim()));

        OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<Tuple2<String, Integer>>("late-data") {};

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = source
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] splits = value.split(",");
                        return Tuple2.of(splits[1].trim(),Integer.valueOf(splits[2].trim()));
                    }
                })
                .keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(outputTag)
                .sum(1);

        result.getSideOutput(outputTag).print("--------side output--------");
        result.print();



        env.execute("WMWindowApp");
    }
}

package com.tyf.flink.wm;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WMWindowApp01 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("sdw2", 9527);
        // 事件时间,domain,traffic  开窗口  求窗口内每个domain出现的次数
        //1000,a,1
        //2000,a,1
        //3000,a,1
        //4999,a,1
        //5000,a,1
        //8888,a,1
        //9998,a,1
        //12000,a,1
        /**
         * 滚动窗口是5秒滚一次
         * 窗口的开始时间和结束时间是:
         * [window_start,window_end)
         * [0000,5000)
         * [5000,10000)
         * 当 watermark >= window_end时  就会触发窗口的执行
         * 当5000,a,1这条数据到达后  watermark为4999 此时 4999>=4999 所以触发窗口内1000~4999这4条数据的计算
         * 当12000,a,1这条数据到达后 watermark为11999 此时 11999>=9999 所以触发窗口内5000~9998这3条数据的计算
         */

        // 1000,a,1
        // 5000,a,1
        // 5555,a,1
        // 7777,a,1
        // 9998,a,1
        // 12000,a,1
        // 14999,a,1
        // 15000,a,1
        // 15001,b,1
        // 19998,a,1
        // 19999,a,1
        // 20000,a,1
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(event.split(",")[0].trim()));
        source.assignTimestampsAndWatermarks(watermarkStrategy)
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] splits = value.split(",");
                        return Tuple2.of(splits[1].trim(),Integer.valueOf(splits[2].trim()));
                    }
                })
                .keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print()
                ;




        env.execute("WMWindowApp");
    }
}

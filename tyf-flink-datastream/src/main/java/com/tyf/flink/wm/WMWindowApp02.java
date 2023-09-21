package com.tyf.flink.wm;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WMWindowApp02 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("sdw2", 9527);
        // 事件时间,domain,traffic  开窗口  求窗口内每个domain出现的次数
        //1000,a,1
        //1999,a,1
        //2000,a,1
        //2222,b,1
        //2999,a,1
        //4000,a,1
        //4999,a,1
        //6000,a,1
        /**
         * 滑动窗口大小为6秒 每2秒滑动一次
         * 窗口的开始时间和结束时间是:
         * [window_start,window_end)
         * [-6000,0)   这个窗口完全看不到
         * [-4000,2000) 这个窗口可以看到0~2000
         * [-2000,4000) 这个窗口可以看到0~4000
         * [0,6000) 这个窗口可以看到0~6000
         *
         * 当 watermark >= window_end时  就会触发窗口的执行
         * 当5000,a,1这条数据到达后  watermark为4999 此时 4999>=4999 所以触发窗口内1000~4999这4条数据的计算
         * 当12000,a,1这条数据到达后 watermark为11999 此时 11999>=9999 所以触发窗口内5000~9998这3条数据的计算
         */

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
                .window(SlidingEventTimeWindows.of(Time.seconds(6),Time.seconds(2)))
                .sum(1)
                .print()
                ;




        env.execute("WMWindowApp");
    }
}

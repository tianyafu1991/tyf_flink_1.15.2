package com.tyf.flink.wm;

import com.tyf.flink.bean.Access;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WMApp01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("sdw2", 9527);
        // 事件时间,domain,traffic
        //1000,a,1
        //2000,a,1
        //3000,a,1
        //4000,a,1
        //3000,a,1
        //2000,a,1
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(event.split(",")[0].trim()));
        source.assignTimestampsAndWatermarks(watermarkStrategy)
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String value) throws Exception {
                        String[] splits = value.split(",");
                        return new Access(Long.parseLong(splits[0].trim()),splits[1].trim(),Double.valueOf(splits[2].trim()));
                    }
                })
                .process(new ProcessFunction<Access, Access>() {
                    @Override
                    public void processElement(Access value, Context ctx, Collector<Access> out) throws Exception {
                        long watermark = ctx.timerService().currentWatermark();
                        System.out.println("该数据是:" + value + " , WM是:" + watermark);
                        out.collect(value);
                    }
                })
                .print();
                
                
        env.execute("WMApp01");
    }
}

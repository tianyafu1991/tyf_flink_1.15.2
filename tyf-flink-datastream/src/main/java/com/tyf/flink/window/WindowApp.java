package com.tyf.flink.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowApp {

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

//        countWindows(env);
//        countKeyByWindows(env);
        tumblingWindow(env);
        env.execute("WindowApp");
    }

    public static void tumblingWindow(StreamExecutionEnvironment env){

    }

    /**
     * nc端输入数据，每一行就一个数字，window为count的 元素个数为5 求和
     * 1
     * 2
     * 3
     * 4
     * 5
     * @param env
     */
    public static void countWindows(StreamExecutionEnvironment env){
        env.socketTextStream("sdw2", 9527)
                .map(x -> Integer.parseInt(x.trim()))
                .countWindowAll(5)
                .sum(0)
                .print();
    }

    /**
     * 因为是keyBy的 所以只有相同的key的元素个数达到指定数量 才会触发执行
     * @param env
     */
    public static void countKeyByWindows(StreamExecutionEnvironment env){
        env.socketTextStream("sdw2", 9527)
                .map(x -> {
                    String[] splits = x.split(",");
                    return new Tuple2(splits[0].trim(),Integer.valueOf(splits[1].trim()));
                })
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(x -> x.f0)
                .countWindow(5)
                .sum(1)
                .print();
    }
}

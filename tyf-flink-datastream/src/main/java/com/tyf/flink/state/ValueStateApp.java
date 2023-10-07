package com.tyf.flink.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 需要使用Flink内置的ValueState来进行状态的管理
 *
 * 当收到的相同的key的元素个数为3，求value的平均值
 *
 * if(xxx.size == 3) {
 *     平均值 = 和 / 元素个数
 * }
 */
public class ValueStateApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String,Long>> list = new ArrayList<>();
        list.add(Tuple2.of("zs",1L));
        list.add(Tuple2.of("zs",7L));
        list.add(Tuple2.of("ww",4L));
        list.add(Tuple2.of("ww",2L));
        list.add(Tuple2.of("zs",7L));

        env.fromCollection(list)
                .keyBy(x -> x.f0)
                .flatMap(new TyfAvgValueStateFunction())
                .print();

        env.execute("ValueStateApp");
    }
}

package com.tyf.flink.basic.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TyfMapFunction implements MapFunction<String, Tuple2<String,Integer>> {
    @Override
    public Tuple2<String, Integer> map(String s) throws Exception {
        return Tuple2.of(s,1);
    }
}

package com.tyf.flink.basic.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class TyfFlatMapFunction implements FlatMapFunction<String,String> {
    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        String[] splits = s.split(",");
        for (String word : splits) {
            collector.collect(word);
        }
    }
}

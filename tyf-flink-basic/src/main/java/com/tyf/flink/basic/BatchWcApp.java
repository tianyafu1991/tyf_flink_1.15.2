package com.tyf.flink.basic;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Flink批处理
 *
 * java.lang.NoClassDefFoundError: org/apache/commons/compress/compressors/CompressorException
 * 添加依赖:<dependency>
 *             <groupId>org.apache.commons</groupId>
 *             <artifactId>commons-compress</artifactId>
 *             <version>1.20</version>
 *         </dependency>
 * 即可解决
 *
 */
public class BatchWcApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = env.readTextFile("data/wc.data");

        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String[] splits = value.split(",");
                for (String word : splits) {
                    collector.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s,1);
            }
        }).groupBy(0)
                .sum(1)
                .print();

    }
}

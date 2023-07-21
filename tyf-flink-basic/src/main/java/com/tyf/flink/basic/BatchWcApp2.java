package com.tyf.flink.basic;


import com.tyf.flink.basic.function.TyfFlatMapFunction;
import com.tyf.flink.basic.function.TyfMapFunction;
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
public class BatchWcApp2 {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = env.readTextFile("data/wc.data");

        source.flatMap(new TyfFlatMapFunction())
                .map(new TyfMapFunction())
                .groupBy(0)
                .sum(1)
                .print();

    }
}

package com.tyf.flink.basic;


import com.tyf.flink.basic.function.TyfFlatMapFunction;
import com.tyf.flink.basic.function.TyfMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
public class BatchWcApp3 {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = env.readTextFile("data/wc.data");

        source.flatMap((String line, Collector<Tuple2<String,Integer>> out)->{
            for (String word : line.split(",")) {
                out.collect(Tuple2.of(word,1));
            }
        })
                .returns(Types.TUPLE(Types.STRING,Types.INT))
//                .returns(new TypeHint<Tuple2<String, Integer>>() {})
//                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .groupBy(0)
                .sum(1)
                .print();

    }
}

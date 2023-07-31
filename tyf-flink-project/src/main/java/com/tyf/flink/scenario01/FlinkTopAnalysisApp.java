package com.tyf.flink.scenario01;

import com.alibaba.fastjson.JSON;
import com.tyf.flink.bean.ProductAccess;
import com.tyf.flink.utils.DateUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 需要加上依赖 否则会报NoClassDefFoundError:org/apache/commons/compress/compressors/zstandard/ZstdCompressorInputStream
 * <dependency>
 *             <groupId>org.apache.commons</groupId>
 *             <artifactId>commons-compress</artifactId>
 *             <version>1.20</version>
 *         </dependency>
 */
public class FlinkTopAnalysisApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 接入数据源
        DataStreamSource<String> source = env.readTextFile("data/productaccess.log");
        // 使用transformation算子进行各种维度的统计分析
        SingleOutputStreamOperator<Tuple3<String, String, Long>> resultStream = source.map(json -> JSON.parseObject(json, ProductAccess.class))
                .map(x -> Tuple3.of(x.getName(), DateUtils.ts2Date(x.getTs(), "yyyyMMdd"), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .keyBy(x -> x.f0)
                .sum(2);
        // 将结果输出到目的地
//        resultStream.print();
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig
                .Builder()
                .setHost("hadoop01")
                .setPort(16379)
                .setDatabase(10)
                .build();
        resultStream.addSink(new RedisSink<Tuple3<String,String,Long>>(conf,new RedisExampleMapper()));
        env.execute("FlinkTopAnalysisApp");
    }

    public static class RedisExampleMapper implements RedisMapper<Tuple3<String, String,Long>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "tyf-product-access");
        }

        @Override
        public String getKeyFromData(Tuple3<String, String, Long> value) {
            return value.f0;
        }

        @Override
        public String getValueFromData(Tuple3<String, String, Long> value) {
            return value.f2.toString();
        }

    }
}

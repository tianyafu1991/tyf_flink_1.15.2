package com.tyf.flink.scenario03;

import com.tyf.flink.utils.FlinkUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * 功能需求：使用Flink对接Kafka的数据源 完成精准一次语义
 */
public class EOSApp01 {

    public static void main(String[] args) throws Exception {


        DataStreamSource<String> streamSource = FlinkUtils.createStream(args, SimpleStringSchema.class);
        kafka2RedisEOS(streamSource);


        FlinkUtils.env.execute("EOSApp01");
    }

    /**
     * 将wc的统计结果写入到redis中
     * @param stream
     */
    public static void kafka2RedisEOS(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<Tuple2<String, Long>> res = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] splits = value.split(",");
                for (String split : splits) {
                    out.collect(Tuple2.of(split.trim(), 1L));
                }
            }
        }).keyBy(x -> x.f0)
                .sum(1);

        res.print();

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("sdw2")
                .setPort(16379)
                .setPassword("NaRT9gnxMKZ6MqA2")
                .setDatabase(5)
                .build();
        res.addSink(new RedisSink<>(conf, new RedisMapper<Tuple2<String, Long>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "tyf-wc-1");
            }

            @Override
            public String getKeyFromData(Tuple2<String, Long> value) {
                return value.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, Long> value) {
                return value.f1 + "";
            }
        }));


    }
}

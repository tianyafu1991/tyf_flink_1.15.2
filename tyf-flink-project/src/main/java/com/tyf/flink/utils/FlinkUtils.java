package com.tyf.flink.utils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class FlinkUtils {

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static <T>DataStreamSource createStream(String[] args, Class<? extends DeserializationSchema<T>> deserializationSchema) throws Exception {

        ParameterTool tool = ParameterTool.fromPropertiesFile(args[0]);
        String brokers = tool.getRequired("brokers");
        String topics = tool.getRequired("topics");
        String groupId = tool.get(ConsumerConfig.GROUP_ID_CONFIG, "tyf-group");
        String enableAutoCommit = tool.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        long checkpointInterval = tool.getLong("checkpoint.interval", 5000L);
        String checkpointStoragePath = tool.get("checkpoint.storage", "file:///D:\\personal\\code\\flink\\tianyafu_flink\\tyf_flink\\chk");
        int restartAttempts = tool.getInt("restart.attempts", 3);
        long delayBetweenAttempts = tool.getLong("delay.between.attempts", 5L);
        String sourceName = tool.get("source.name");


        env.enableCheckpointing(checkpointInterval);
//        env.getCheckpointConfig().setCheckpointStorage(checkpointStoragePath);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, delayBetweenAttempts));
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        KafkaSource<T> source = KafkaSource
                .<T>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit)  // 对于Flink来说，offset在State管理中来完成 不需要保存到Kafka的特殊的topic中 所以给个false
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(deserializationSchema.newInstance())
                .build();

        DataStreamSource<T> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), sourceName);
        return stream;
    }
}

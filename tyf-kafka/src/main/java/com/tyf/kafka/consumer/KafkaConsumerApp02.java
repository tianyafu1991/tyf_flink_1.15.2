package com.tyf.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * 重平衡
 */
public class KafkaConsumerApp02 {

    public static final String BROKERS = "mdw:9092,sdw1:9092,sdw2:9092";
    public static final String TOPIC = "tyf-7-1";
    public static final String GROUP_ID = "tyf-group";


    private KafkaConsumer<String,String> consumer;

    @Before
    public void setUp(){
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumer = new KafkaConsumer<String, String>(props);
    }


    @Test
    public void test01(){
        consumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {
            /**
             * 再平衡的时候  消费者需要先废弃原来的订阅
             * @param partitions
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("取消分区: " + partitions);
            }

            /**
             * 再平衡的时候  消费者废弃原来的订阅后 还需要分配新的订阅
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("被分配新的分区: " + partitions);
            }
        });
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100L);
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();

                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();

                System.out.println("key: " + key + " , value: " + value + " , topic: " + topic + " , partition: " + partition + " , offset: " + offset);
            }

        }
    }

    @Test
    public void test02(){
        consumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {
            /**
             * 再平衡的时候  消费者需要先废弃原来的订阅
             * @param partitions
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("取消分区: " + partitions);
            }

            /**
             * 再平衡的时候  消费者废弃原来的订阅后 还需要分配新的订阅
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("被分配新的分区: " + partitions);
            }
        });
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100L);
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();

                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();

                System.out.println("key: " + key + " , value: " + value + " , topic: " + topic + " , partition: " + partition + " , offset: " + offset);
            }

        }
    }

    @Test
    public void test03(){
        consumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {
            /**
             * 再平衡的时候  消费者需要先废弃原来的订阅
             * @param partitions
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("取消分区: " + partitions);
            }

            /**
             * 再平衡的时候  消费者废弃原来的订阅后 还需要分配新的订阅
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("被分配新的分区: " + partitions);
            }
        });
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100L);
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();

                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();

                System.out.println("key: " + key + " , value: " + value + " , topic: " + topic + " , partition: " + partition + " , offset: " + offset);
            }

        }
    }


    @After
    public void tearDown(){
        if(null != consumer){
            consumer.close();
        }
    }

}

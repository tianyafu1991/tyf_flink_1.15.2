package com.tyf.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerApp {

    public static final String BROKERS = "mdw:9092,sdw1:9092,sdw2:9092";
    public static final String TOPIC = "tyf-3-1";
    public static final String GROUP_ID = "tyf-group";


    private KafkaConsumer<String,String> consumer;

    @Before
    public void setUp(){
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumer = new KafkaConsumer<String, String>(props);
    }

    /**
     * 普通的消费数据
     */
    @Test
    public void test01(){
        List<String> topics = new ArrayList<>();
        topics.add(TOPIC);
        consumer.subscribe(topics);
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

    /**
     * 指定消费某个分区
     */
    @Test
    public void test02(){
        List<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(new TopicPartition(TOPIC,2));
        consumer.assign(topicPartitions);
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

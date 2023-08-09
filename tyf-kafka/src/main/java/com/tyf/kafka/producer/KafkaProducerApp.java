package com.tyf.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * 生产者
 * kafka-topics.sh --list --bootstrap-server hadoop01:9092
 *
 * kafka-topics.sh --describe --bootstrap-server hadoop01:9092 --topic tianyafu11
 */
public class KafkaProducerApp {

    private KafkaProducer<String, String> producer;
    public static String BROKERS = "hadoop01:9092";
    public static String TOPIC = "tianyafu11";

    @Before
    public void setup(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);

        // 如果你想去提升你的Kafka的性能问题，是不是该从如下环节入手
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 3);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        props.put(ProducerConfig.ACKS_CONFIG, 1);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<String, String>(props);
    }

    /**
     * 普通的异步发送
     */
    @Test
    public void test01(){
        for (int i = 20; i < 25; i++) {
            producer.send(new ProducerRecord<>(TOPIC,"tyf-"+i));
        }
    }

    /**
     * 带回调函数的异步发送
     */
    @Test
    public void test02(){
        for (int i = 30; i < 35; i++) {
            producer.send(new ProducerRecord<>(TOPIC,"tyf-"+i),new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if(e != null) {
                        System.out.println("发送失败..." + e.getMessage());
                    } else {
                        System.out.println("发送成功，topic:" + metadata.topic() + " ,分区是:" + metadata.partition() + " ,offset是:" + metadata.offset());
                    }
                }
            });
        }
    }

    /**
     * 同步发送
     * 会阻塞
     * send方法会返回一个Future对象  只有发送成功后 才能通过get()方法得到Future对象 然后才会接着发下一条数据
     * 生产上不用这种方式
     */
    @Test
    public void test03() throws Exception{
        for (int i = 40; i < 45; i++) {
            producer.send(new ProducerRecord<>(TOPIC,"tyf-"+i)).get();
        }
    }

    @After
    public void teardown(){
        if (null != producer){
            producer.close();
        }
    }



    public static void main(String[] args) {
        /*String topic = "tianyafu11";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092");
        *//*props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);*//*
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>(topic,"tyf-"+i));
        }

        producer.close();*/

        System.out.println(StringSerializer.class.getName());

    }
}

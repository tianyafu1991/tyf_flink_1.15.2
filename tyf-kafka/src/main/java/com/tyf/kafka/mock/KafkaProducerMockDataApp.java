package com.tyf.kafka.mock;

import com.tyf.kafka.consumer.KafkaConsumerApp;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerMockDataApp {

    public static void sendMsg(String brokers, String topic, int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        // 如果你想去提升你的Kafka的性能问题，是不是该从如下环节入手
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 3);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < count; i++) {
            producer.send(new ProducerRecord(topic, "tyf-" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        System.out.println("发送失败..." + e.getMessage());
                    } else {
                        System.out.println("发送成功，topic:" + metadata.topic() + " ,分区是:" + metadata.partition() + " ,offset是:" + metadata.offset());
                    }
                }
            });
            Thread.sleep(100L);
        }
        producer.close();
    }

    public static void sendMsgWithPartition(String brokers, String topic, int partitionId, int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        // 如果你想去提升你的Kafka的性能问题，是不是该从如下环节入手
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 3);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < count; i++) {
            producer.send(new ProducerRecord(topic, partitionId, "", "tyf-" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        System.out.println("发送失败..." + e.getMessage());
                    } else {
                        System.out.println("发送成功，topic:" + metadata.topic() + " ,分区是:" + metadata.partition() + " ,offset是:" + metadata.offset());
                    }
                }
            });
            Thread.sleep(100L);
        }
        producer.close();
    }


    public static void main(String[] args) throws Exception {
        sendMsg(KafkaConsumerApp.BROKERS,KafkaConsumerApp.TOPIC,10);
//        sendMsgWithPartition(KafkaConsumerApp.BROKERS, KafkaConsumerApp.TOPIC, 2, 10);
    }
}

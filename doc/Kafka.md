# Kafka

## 查看topics
```shell
kafka-topics --zookeeper mdw:2181,sdw1:2181,sdw2:2181/kafka --list
```

## 创建topic
```shell
kafka-topics --create \
--topic tyfflink \
--zookeeper mdw:2181,sdw1:2181,sdw2:2181/kafka \
--partitions 3 \
--replication-factor 3


kafka-topics --create \
--topic tyfflink02 \
--zookeeper mdw:2181,sdw1:2181,sdw2:2181/kafka \
--partitions 3 \
--replication-factor 3
```

## 启动producer
```shell
kafka-console-producer \
--broker-list mdw:9092,sdw1:9092,sdw2:9092 \
--topic tyfflink
```


## 启动消费者
```shell
kafka-console-consumer \
--bootstrap-server mdw:9092,sdw1:9092,sdw2:9092 \
--from-beginning \
--topic tyfflink
```
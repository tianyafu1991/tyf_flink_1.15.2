package com.tyf.flink.sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class FlinkRedisSinkApp extends RichSinkFunction<Tuple3<String,String,Long>> {

    Jedis jedis = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new Jedis("192.168.26.200",16379);
        jedis.select(9);
    }

    @Override
    public void invoke(Tuple3<String, String, Long> value, Context context) throws Exception {
        if(!jedis.isConnected()){jedis.connect();}
        jedis.hset("tyf-redis-" + value.f1,value.f0,value.f2 + "");

    }

    @Override
    public void close() throws Exception {
        if(null != jedis) jedis.close();
    }
}

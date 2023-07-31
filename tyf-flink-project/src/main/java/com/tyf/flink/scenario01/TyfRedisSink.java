package com.tyf.flink.scenario01;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class TyfRedisSink extends RichSinkFunction<Tuple3<String, String, Long>> {

    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new Jedis("hadoop01",16379);
        jedis.select(11);
    }

    @Override
    public void close() throws Exception {
        if(null != jedis){
            jedis.close();
        }
    }

    @Override
    public void invoke(Tuple3<String, String, Long> value, Context context) throws Exception {

        if (!jedis.isConnected()){
            jedis.connect();
        }
        jedis.hset(value.f0, value.f1, value.f2.toString());
    }
}

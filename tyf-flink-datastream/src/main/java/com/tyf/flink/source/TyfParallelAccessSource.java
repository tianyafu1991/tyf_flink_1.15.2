package com.tyf.flink.source;

import com.tyf.flink.bean.Access;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;


public class TyfParallelAccessSource implements ParallelSourceFunction<Access> {

    private volatile boolean isRunning = true;


    String[] users = {"tyf", "zs", "ls"};

    @Override
    public void run(SourceContext<Access> ctx) throws Exception {

        Random rand = new Random();

        while (isRunning) {
            long time = System.currentTimeMillis();
            String user = users[rand.nextInt(users.length)];
            double traffic = rand.nextDouble() + 1000;
            ctx.collect(new Access(time, user, traffic));
            Thread.sleep(1000);
        }
    }


    @Override
    public void cancel() {
        isRunning = false;
    }
}

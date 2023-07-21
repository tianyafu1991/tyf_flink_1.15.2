package com.tyf.flink.source;

import com.tyf.flink.bean.Access;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;


public class TyfRichParallelAccessSource extends RichParallelSourceFunction<Access> {

    private volatile boolean isRunning = true;


    String[] users = {"tyf", "zs", "ls"};

    @Override
    public void open(Configuration parameters) throws Exception {
//        System.out.println("open invoked........");
        RuntimeContext context = getRuntimeContext();
        String taskName = context.getTaskName();
        int subtaskId = context.getIndexOfThisSubtask();
        System.out.println(taskName + "=======>" +subtaskId);
    }

    @Override
    public void close() throws Exception {
        System.out.println("close invoked........");
    }


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

package com.tyf.flink.state;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

public class TyfMapFunction implements MapFunction<String,String>, CheckpointedFunction {

    ListState<String> listState;
    FastDateFormat format = FastDateFormat.getInstance("hh:MM:ss");

    @Override
    public String map(String value) throws Exception {
        System.out.println("map invoked");
        if("zs".equals(value)){
            throw new RuntimeException("程序抛错了");
        }else {
            listState.add(value);
            StringBuilder builder = new StringBuilder();
            for (String s : listState.get()) {
                builder.append(s);
            }
            return builder.toString();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println(format.format(System.currentTimeMillis()) + "snapshotState invoked.... checkPointId:" + context.getCheckpointId());

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("initializeState invoked");
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();
        listState = operatorStateStore.getListState(new ListStateDescriptor("", String.class));

    }
}

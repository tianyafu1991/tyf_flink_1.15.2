package com.tyf.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.ValueSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class TyfAvgValueStateFunction extends RichFlatMapFunction<Tuple2<String,Long>,Tuple2<String,Double>> {
    /**
     * 泛型第一个元素存放和 第二个元素存放次数
     */
    private transient ValueState<Tuple2<Long,Long>> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        valueState = getRuntimeContext().getState(new ValueStateDescriptor("value", Types.TUPLE(Types.LONG,Types.LONG)));
    }

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Double>> out) throws Exception {
        Tuple2<Long, Long> state = valueState.value();
        if(null == state){
            state = Tuple2.of(0L,0L);
        }
        Tuple2<Long, Long> current = Tuple2.of(state.f0 + value.f1, state.f1 + 1);
        valueState.update(current);
        if(current.f1 >= 3){
            out.collect(Tuple2.of(value.f0,current.f0 * 1.0 / current.f1));
            // 清理状态
            valueState.clear();
        }

    }
}

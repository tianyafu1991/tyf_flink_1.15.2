package com.tyf.flink.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TyfAvgFunction implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {
    /**
     * Creates a new accumulator, starting a new aggregate.
     *
     * <p>The new accumulator is typically meaningless unless a value is added via {@link
     * #add(Object, Object)}.
     *
     * <p>The accumulator is the state of a running aggregation. When a program has multiple
     * aggregates in progress (such as per key and window), the state (per key and window) is the
     * size of the accumulator.
     *
     * @return A new accumulator, corresponding to an empty aggregate.
     */
    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return Tuple2.of(0L, 0L);
    }

    /**
     * Adds the given input value to the given accumulator, returning the new accumulator value.
     *
     * <p>For efficiency, the input accumulator may be modified and returned.
     *
     * @param value       The value to add
     * @param accumulator The accumulator to add the value to
     * @return The accumulator with the updated state
     */
    @Override
    public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
        System.out.println("add invoked..." + value.f0 + "==============>" + value.f1);
        return Tuple2.of(accumulator.f0 + value.f1, accumulator.f1 + 1);
    }

    /**
     * Gets the result of the aggregation from the accumulator.
     *
     * @param accumulator The accumulator of the aggregation
     * @return The final aggregation result.
     */
    @Override
    public Double getResult(Tuple2<Long, Long> accumulator) {
        return Double.valueOf(accumulator.f0) / accumulator.f1;
    }

    /**
     * Merges two accumulators, returning an accumulator with the merged state.
     *
     * <p>This function may reuse any of the given accumulators as the target for the merge and
     * return that. The assumption is that the given accumulators will not be used any more after
     * having been passed to this function.
     *
     * @param a An accumulator to merge
     * @param b Another accumulator to merge
     * @return The accumulator with the merged state
     */
    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        return null;
    }
}

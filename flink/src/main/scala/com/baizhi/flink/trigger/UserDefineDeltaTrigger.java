package com.baizhi.trigger;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class UserDefineDeltaTrigger<T, W extends Window> extends Trigger<T, W> {
    private static final long serialVersionUID = 1L;

    private final DeltaFunction<T> deltaFunction;
    private final double threshold;
    private final ValueStateDescriptor<T> stateDesc;

    private UserDefineDeltaTrigger(double threshold, DeltaFunction<T> deltaFunction, TypeSerializer<T> stateSerializer) {
        this.deltaFunction = deltaFunction;
        this.threshold = threshold;
        this.stateDesc = new ValueStateDescriptor("last-element", stateSerializer);

    }

    @Override
    public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
        ValueState<T> lastElementState = ctx.getPartitionedState(stateDesc);
        if (lastElementState.value() == null) {
            lastElementState.update(element);
            return TriggerResult.CONTINUE;
        }
        if (deltaFunction.getDelta(lastElementState.value(), element) > this.threshold) {
            lastElementState.update(element);
            //触发并清空
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(stateDesc).clear();
    }

    @Override
    public String toString() {
        return "DeltaTrigger(" +  deltaFunction + ", " + threshold + ")";
    }

    /**
     * Creates a delta trigger from the given threshold and {@code DeltaFunction}.
     *
     * @param threshold The threshold at which to trigger.
     * @param deltaFunction The delta function to use
     * @param stateSerializer TypeSerializer for the data elements.
     *
     * @param <T> The type of elements on which this trigger can operate.
     * @param <W> The type of {@link Window Windows} on which this trigger can operate.
     */
    public static <T, W extends Window> UserDefineDeltaTrigger<T, W> of(double threshold, DeltaFunction<T> deltaFunction, TypeSerializer<T> stateSerializer) {
        return new UserDefineDeltaTrigger(threshold, deltaFunction, stateSerializer);
    }
}

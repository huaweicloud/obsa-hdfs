package com.huaweicloud.obs.flinktest;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * 功能描述
 *
 * @since 2021-05-12
 */
public class EndSequenceGenerator implements Serializable {

    private static final long serialVersionUID = 3885103821540774586L;

    private final long start;

    private final long end;

    private transient ListState<Long> checkpointedState;

    protected transient Deque<Long> valuesToEmit;

    /**
     * Creates a DataGenerator that emits all numbers from the given interval exactly once.
     *
     * @param start Start of the range of numbers to emit.
     * @param end   End of the range of numbers to emit.
     */
    public EndSequenceGenerator(long start, long end) {
        this.start = start;
        this.end = end;
        this.valuesToEmit = new ArrayDeque<>();
    }

    public void open(
        String name, FunctionInitializationContext context, RuntimeContext runtimeContext)
        throws Exception {
        Preconditions.checkState(
            this.checkpointedState == null,
            "The " + getClass().getSimpleName() + " has already been initialized.");

        this.checkpointedState =
            context.getOperatorStateStore()
                .getListState(
                    new ListStateDescriptor<>(
                        name + "-sequence-state", LongSerializer.INSTANCE));
        this.valuesToEmit = new ArrayDeque<>();
        if (context.isRestored()) {
            // upon restoring

            for (Long v : this.checkpointedState.get()) {
                this.valuesToEmit.add(v);
            }
        } else {
            // the first time the job is executed
            final int stepSize = runtimeContext.getNumberOfParallelSubtasks();
            final int taskIdx = runtimeContext.getIndexOfThisSubtask();
            final long congruence = start + taskIdx;

            long totalNoOfElements = Math.abs(end - start + 1);
            final int baseSize = safeDivide(totalNoOfElements, stepSize);
            final int toCollect =
                (totalNoOfElements % stepSize > taskIdx) ? baseSize + 1 : baseSize;

            for (long collected = 0; collected < toCollect; collected++) {
                this.valuesToEmit.add(collected * stepSize + congruence);
            }
        }
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(
            this.checkpointedState != null,
            "The " + getClass().getSimpleName() + " state has not been properly initialized.");

        this.checkpointedState.clear();
        for (Long v : this.valuesToEmit) {
            this.checkpointedState.add(v);
        }
    }

    public boolean hasNext() {
        return !this.valuesToEmit.isEmpty();
    }

    public String next() {
        return valuesToEmit.poll().toString();
    }

    private static int safeDivide(long left, long right) {
        Preconditions.checkArgument(right > 0);
        Preconditions.checkArgument(left >= 0);
        Preconditions.checkArgument(left <= Integer.MAX_VALUE * right);
        return (int) (left / right);
    }
}

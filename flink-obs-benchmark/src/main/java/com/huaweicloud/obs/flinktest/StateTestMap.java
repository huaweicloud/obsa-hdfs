package com.huaweicloud.obs.flinktest;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.LinkedList;
import java.util.List;

/**
 * 功能描述
 *
 * @since 2021-05-12
 */
public class StateTestMap extends RichMapFunction<String, String> implements CheckpointedFunction {
    private static final long serialVersionUID = 2385221919708815302L;

    private transient ListState<String> listState = null;

    private List<String> buffer;

    private long stateSize;

    private int recordLength = 200;

    private int totalTasks;

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        listState.clear();

        long size = stateSize / totalTasks < recordLength ? recordLength : stateSize / totalTasks;
        long threshold = size / recordLength;
        int bufferSize = buffer.size();
        for (int i = 0; i < threshold - bufferSize; i++) {
            buffer.add(DataGenerator.getRecord(recordLength));
        }
        listState.addAll(buffer);
    }

    public StateTestMap(ParameterTool params) {
        this.stateSize = params.getLong("stateSize",  1024);
        this.buffer = new LinkedList<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.totalTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        super.open(parameters);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("state-benchmark",
            TypeInformation.of(new TypeHint<String>() {
            }));
        listState = context.getOperatorStateStore().getListState(listStateDescriptor);
        if (context.isRestored()) {
            for (String b : listState.get()) {
                buffer.add(b);
            }
        }
    }

    @Override
    public String map(String value) throws Exception {
        // int size = bufferState.size();
        // if(size>= threshold){
        //     for (int i=0;i<size-threshold;i++){
        //         bufferState.poll();
        //     }
        // }
        // bufferState.add(value);
        return value;
    }
}

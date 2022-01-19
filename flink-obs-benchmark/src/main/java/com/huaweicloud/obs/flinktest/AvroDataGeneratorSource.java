/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huaweicloud.obs.flinktest;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * A data generator source that abstract data generator. It can be used to easy startup/test for
 * streaming job and performance testing. It is stateful, re-scalable, possibly in parallel.
 */

public class AvroDataGeneratorSource extends RichParallelSourceFunction<BaseEvent> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private long startTime;

    private long totalTime;

    private long totalRecords;       // Total records this task plan to sent. -1 means no limit.

    private long totalRecordsPerTask;

    private long recordsPerSecond; // Define how many records will be sent on each round

    private long recordsPerSecondPerTask;

    private int recordSize;

    private long recordsCounter;

    private int totalTasks;

    transient volatile boolean isRunning;

    /**
     * Creates a source that emits records by {@link DataGenerator} without controlling emit rate.
     */
    public AvroDataGeneratorSource(ParameterTool params) {
        this.totalRecords = params.getLong("totalRecords", -1);
        this.totalTime = params.getLong("totalTime", -1);
        this.recordsPerSecond = params.getLong("recordsPerSecond", -1);
        this.recordSize = params.getInt("recordSize", 1024);

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.totalTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        this.recordsPerSecondPerTask = recordsPerSecond / totalTasks < 1 ? 1 : recordsPerSecond / totalTasks;
        this.totalRecordsPerTask = totalRecords / totalTasks;
        this.isRunning = true;
        this.startTime = System.currentTimeMillis();

        super.open(parameters);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("----------snapshotState------------------");
    }

    @Override
    public void run(SourceContext<BaseEvent> ctx) throws Exception {

        long nextReadTime = System.currentTimeMillis();
        while (isRunning) {
            if (recordsPerSecond == -1) {
                if (isRunning && isRecordValid() && isTimeValid()) {
                    synchronized (ctx.getCheckpointLock()) {
                        recordsCounter++;
                        ctx.collect(AvroDataGenerator.getRecord(recordSize));
                    }
                } else {
                    return;
                }
            } else {
                for (int i = 0; i < recordsPerSecondPerTask; i++) {
                    if (isRunning && isRecordValid() && isTimeValid()) {
                        synchronized (ctx.getCheckpointLock()) {
                            recordsCounter++;
                            ctx.collect(AvroDataGenerator.getRecord(recordSize));
                        }
                    } else {
                        return;
                    }
                }

                nextReadTime += 1000;
                long toWaitMs = nextReadTime - System.currentTimeMillis();
                while (toWaitMs > 0) {
                    Thread.sleep(toWaitMs);
                    toWaitMs = nextReadTime - System.currentTimeMillis();
                }
            }
        }
    }

    // Check round times, if it's bigger than total rounds, terminate data generator
    private boolean isTimeValid() {
        return (-1 == totalTime) || ((System.currentTimeMillis() - startTime) < totalTime);
    }

    // Check sent record number, if it's bigger than total records, terminate data generator
    private boolean isRecordValid() {
        return (-1 == totalRecords) || (recordsCounter < totalRecordsPerTask);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

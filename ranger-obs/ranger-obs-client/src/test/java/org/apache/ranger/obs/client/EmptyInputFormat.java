/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.client;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EmptyInputFormat extends InputFormat<IntWritable, IntWritable> {
    @Override
    public List<InputSplit> getSplits(JobContext context) {
        List<InputSplit> ret = new ArrayList<InputSplit>();
        int numSplits = context.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
        for (int i = 0; i < numSplits; ++i) {
            ret.add(new EmptySplit());
        }
        return ret;
    }

    @Override
    public RecordReader<IntWritable, IntWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new EmptyRecordReader();
    }

    private static class EmptyRecordReader extends RecordReader<IntWritable, IntWritable> {
        private int records = 2;

        private IntWritable key = null;

        private IntWritable value = null;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            records = records - 1;
            key = new IntWritable(1);
            value = new IntWritable(1);
            return records > 0;
        }

        public IntWritable getCurrentKey() {
            return key;
        }

        @Override
        public IntWritable getCurrentValue() {
            return value;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public float getProgress() throws IOException {
            return 0.0f;
        }
    }

    public static class EmptySplit extends InputSplit implements Writable {
        @Override
        public void write(DataOutput out) throws IOException {
        }

        @Override
        public void readFields(DataInput in) throws IOException {
        }

        @Override
        public long getLength() {
            return 0L;
        }

        @Override
        public String[] getLocations() {
            return new String[0];
        }
    }
}


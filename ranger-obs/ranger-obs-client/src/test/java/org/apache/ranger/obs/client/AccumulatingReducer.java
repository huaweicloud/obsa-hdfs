/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * description
 *
 * @since 2021-10-16
 */
public class AccumulatingReducer extends Reducer<Text, Text, Text, Text> {
    static final String EACH_TIME = "each time:";

    static final String TOTAL_TIME = "total";

    private static final Logger LOG = LoggerFactory.getLogger(AccumulatingReducer.class);

    Histogram histogram;

    boolean detail = false;

    int maps = 1;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        detail = conf.getBoolean("ranger.obs.bench.detail", false);
        maps = conf.getInt(MRJobConfig.NUM_MAPS, 1);
        LOG.info("map task count:" + maps);
        histogram = new Histogram();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String field = key.toString();
        if (EACH_TIME.equals(field)) {
            histogram.getMeasurements(context, values, detail);
            context.write(new Text("Maps"), new Text(String.valueOf(maps)));
            context.write(new Text("Operations"), new Text(String.valueOf(histogram.getOperations())));
            context.write(new Text("Totallatency"), new Text(String.valueOf(histogram.getTotallatency())));
            context.write(new Text("MeanLatency"), new Text(String.valueOf(histogram.getMean())));
            context.write(new Text("MaxLatency"), new Text(String.valueOf(histogram.getMax())));
            context.write(new Text("MinLatency"), new Text(String.valueOf(histogram.getMin())));
            context.write(new Text("Variance"), new Text(String.valueOf(histogram.getVariance())));
            context.write(new Text("PercentileLatency95"),
                new Text(String.valueOf(histogram.getPercentileLatency95())));
            context.write(new Text("PercentileLatency99"),
                new Text(String.valueOf(histogram.getPercentileLatency99())));

            // return;
        }
        if (TOTAL_TIME.equals(field)) {
            Iterator<Text> iterator = values.iterator();
            long sum = 0;
            while (iterator.hasNext()) {
                Text next = iterator.next();
                sum += Long.parseLong(next.toString());
            }
            context.write(new Text("total"), new Text(String.valueOf(sum)));
        }
    }
}


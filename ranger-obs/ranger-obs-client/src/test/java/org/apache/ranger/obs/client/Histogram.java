/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.client;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Take measurements and maintain a histogram of a given metric, such as READ LATENCY.
 *
 * @author cooperb
 */
public class Histogram {
    public static final int BUCKETS_DEFAULT = 1000;

    int _buckets;

    int[] histogram;

    int histogramoverflow;

    int operations;

    long totallatency;

    double totalsquaredlatency;

    double mean;

    double variance;

    int min;

    int max;

    int percentileLatency95;

    int percentileLatency99;

    public Histogram() {
        _buckets = BUCKETS_DEFAULT;
        histogram = new int[_buckets];
        histogramoverflow = 0;
        operations = 0;
        totallatency = 0;
        totalsquaredlatency = 0;
        min = -1;
        max = -1;
    }

    public synchronized void measure(int latency) {
        if (latency>=_buckets) {
            histogramoverflow++;
        } else {
            histogram[latency]++;
        }
        operations++;
        totallatency += latency;
        totalsquaredlatency += ((double)latency) * ((double)latency);

        if ( (min<0) || (latency<min) ) {
            min=latency;
        }
        if ( (max<0) || (latency>max) ) {
            max=latency;
        }
    }

    public void exportMeasurements(boolean detail) {
        if (detail) {
            for (int i = 0; i < _buckets; i++) {
                System.out.println(i + "," + histogram[i]);
            }
            System.out.println(">" + _buckets + "," + histogramoverflow);
        }

        double mean = totallatency/((double)operations);
        double variance = totalsquaredlatency/((double)operations) - (mean * mean);
        System.out.println("Operations,"+operations);
        System.out.println("Totallatency,"+totallatency);
        System.out.println("AverageLatency,"+ mean);
        System.out.println("MaxLatency,"+ max);
        System.out.println("MinLatency,"+ min);
        System.out.println("LatencyVariance,"+variance);
        int opcounter = 0;
        boolean done95th = false;
        for (int i = 0; i < _buckets; i++) {
            opcounter += histogram[i];
            if ((!done95th) && (((double) opcounter) / ((double) operations) >= 0.95)) {
                System.out.println("percentileLatency95," + i);
                done95th = true;
            }
            if (((double) opcounter) / ((double) operations) >= 0.99) {
                System.out.println("percentileLatency99," + i);
                break;
            }
        }
    }

    public void getMeasurements(Reducer.Context context, Iterable<Text> values, boolean detail)
        throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            int latency = Integer.parseInt(iterator.next().toString());
            if (latency >= _buckets) {
                histogramoverflow++;
            } else {
                histogram[latency]++;
            }
            operations++;
            totallatency += latency;
            totalsquaredlatency += ((double) latency) * ((double) latency);
            if ((min < 0) || (latency < min)) {
                min = latency;
            }
            if ((max < 0) || (latency > max)) {
                max = latency;
            }
        }
        mean = totallatency / ((double) operations);
        variance = totalsquaredlatency / ((double) operations) - (mean * mean);

        int opcounter = 0;
        boolean done95th = false;
        for (int i = 0; i < _buckets; i++) {
            opcounter += histogram[i];
            if ((!done95th) && (((double) opcounter) / ((double) operations) >= 0.95)) {
                percentileLatency95 = i;
                done95th = true;
            }
            if (((double) opcounter) / ((double) operations) >= 0.99) {
                percentileLatency99 = i;
                break;
            }
        }
        if (detail){
            for (int i=0; i<_buckets; i++) {
                context.write(new Text(String.valueOf(i)), histogram[i]);
            }
            context.write(new Text(">" + _buckets + ","), histogramoverflow);
        }
    }

    public int getOperations() {
        return operations;
    }

    public long getTotallatency() {
        return totallatency;
    }

    public double getTotalsquaredlatency() {
        return totalsquaredlatency;
    }

    public double getMean() {
        return mean;
    }

    public double getVariance() {
        return variance;
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

    public int getPercentileLatency95() {
        return percentileLatency95;
    }

    public int getPercentileLatency99() {
        return percentileLatency99;
    }
}

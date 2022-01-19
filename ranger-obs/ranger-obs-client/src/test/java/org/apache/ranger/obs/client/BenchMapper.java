/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.obs.security.AccessType;
import org.apache.hadoop.fs.obs.security.RangerAuthorizeProvider;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * description
 *
 * @since 2021-10-16
 */
public class BenchMapper extends Mapper<IntWritable, IntWritable, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(BenchMapper.class);

    int concurrency = 1;

    long requests = 1;

    long threadopcount;

    String bucket = "";

    String object = "";

    AccessType action = AccessType.READ;

    long runtime = 0;
    long startline = 0;
    boolean longrun = false;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        concurrency = conf.getInt("ranger.obs.bench.concurrency", 1);
        requests = conf.getLong("ranger.obs.bench.requests", 1);
        threadopcount = requests / concurrency;
        String type = conf.get("ranger.obs.bench.action");
        if ("read".equals(type)) {
            action = AccessType.READ;
        } else {
            action = AccessType.WRITE;
        }
        Path control_path = new Path(conf.get("ranger.obs.bench.path"));
        bucket = control_path.toUri().getAuthority();
        object = control_path.toString();
        runtime = conf.getLong("ranger.obs.bench.runtime", 0);
        startline = conf.getLong("ranger.obs.bench.startline", 0);
        longrun = conf.getBoolean("ranger.obs.bench.longrun", false);
    }

    @Override
    public void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {

        final List<Thread> threads = new ArrayList<Thread>(concurrency);
        CountDownLatch countDownLatch = new CountDownLatch(concurrency);

        for (int id = 0; id < concurrency; id++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    RangerAuthorizeProvider rangerAuthorizeProvider = new RangerAuthorizeProvider();
                    try {
                        rangerAuthorizeProvider.init(context.getConfiguration());
                        countDownLatch.await();

                        if(startline>0){
                            long start = System.currentTimeMillis();
                            if (start < startline){
                                Thread.sleep(startline-start);
                            }
                        }
                    } catch (InterruptedException | IOException e) {
                        throw new RuntimeException(e);
                    }

                    try {
                        long init_time = System.currentTimeMillis();
                        long expect_end_time = init_time + runtime * 1000;
                        long et = init_time;
                        long index = 0;
                        while (et <= expect_end_time || (index < threadopcount)) {
                            long st = System.currentTimeMillis();
                            rangerAuthorizeProvider.isAuthorized(bucket, object, action);
                            index++;
                            et = System.currentTimeMillis();
                            context.write(new Text(AccumulatingReducer.EACH_TIME),
                                new Text(String.valueOf(et - st)));
                            if (longrun){
                                Thread.sleep(1000);
                            }
                        }
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }, "clientThread" + id);
            threads.add(t);
        }

        long start_time = System.currentTimeMillis();
        for (Thread th : threads) {
            th.start();
            countDownLatch.countDown();
        }
        for (Thread th : threads) {
            try {
                th.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        long end_time = System.currentTimeMillis();
        long execTime = end_time - start_time;
        context.write(new Text(AccumulatingReducer.TOTAL_TIME), new Text(String.valueOf(execTime)));
        LOG.info("threads finished!!!" + execTime);
    }
}


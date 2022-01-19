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
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * description
 *
 * @since 2021-10-16
 */
public class MiniBenchmark extends Mapper<IntWritable, IntWritable, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(MiniBenchmark.class);

    int concurrency = 1;

    int requests = 1;

    String bucket = "";

    String object = "";

    AccessType action = AccessType.READ;

    long runtime = 0;

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Missing arguments.");
            return;
        }

        int concurrency = 1;
        long requests = 1;
        String path = "";
        String result = "";
        String type = "read";
        long runtime = 0;
        int numMapper = 1;
        boolean detail = false;
        boolean longrun = false;
        for (int i = 0; i < args.length; i++) { // parse command line
            if (StringUtils.toLowerCase(args[i]).startsWith("-concurrency")) {
                concurrency = Integer.parseInt(args[++i]);
            } else if (args[i].equalsIgnoreCase("-requests")) {
                requests = Long.parseLong(args[++i]);
            } else if (args[i].equalsIgnoreCase("-mapper")) {
                numMapper = Integer.parseInt(args[++i]);
            } else if (args[i].equalsIgnoreCase("-path")) {
                path = args[++i];
            } else if (args[i].equalsIgnoreCase("-action")) {
                type = args[++i];
            } else if (args[i].equalsIgnoreCase("-output")) {
                result = args[++i];
            } else if (args[i].equalsIgnoreCase("-runtime")) {
                runtime = Long.parseLong(args[++i]);
            } else if (args[i].equalsIgnoreCase("-s")) {
                detail = true;
            }  else if (args[i].equalsIgnoreCase("-l")) {
                longrun = true;
            } else {
                System.err.println("Illegal argument: " + args[i]);
                return;
            }
        }


        Configuration config = new Configuration();
        Path control_path = new Path(path);
        String bucket = control_path.toUri().getAuthority();
        String object = control_path.toString();
        AccessType action = AccessType.READ;
        if ("read".equals(type)) {
            action = AccessType.READ;
        } else {
            action = AccessType.WRITE;
        }
        Histogram histogram = new Histogram();
        long threadopcount = requests / concurrency;


        final List<Thread> threads = new ArrayList<Thread>(concurrency);
        CountDownLatch countDownLatch = new CountDownLatch(concurrency);

        for (int id = 0; id < concurrency; id++) {
            AccessType finalAction = action;
            long finalRuntime = runtime;
            boolean finalLongrun = longrun;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    RangerAuthorizeProvider rangerAuthorizeProvider = new RangerAuthorizeProvider();
                    try {
                        rangerAuthorizeProvider.init(config);
                        countDownLatch.await();
                    } catch (InterruptedException | IOException e) {
                        throw new RuntimeException(e);
                    }

                    try {
                        long init_time = System.currentTimeMillis();
                        long expect_end_time = init_time + finalRuntime * 1000;
                        long et = init_time;
                        long index = 0;
                        while (et <= expect_end_time || (index < threadopcount)) {
                            long st = System.currentTimeMillis();
                            rangerAuthorizeProvider.isAuthorized(bucket, object, finalAction);
                            index++;
                            et = System.currentTimeMillis();
                            if(finalLongrun){
                                Thread.sleep(1000);
                            }
                            histogram.measure((int)(et-st));
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
        histogram.exportMeasurements(detail);
        long execTime = end_time - start_time;
        System.out.println("total," + execTime);

    }
}


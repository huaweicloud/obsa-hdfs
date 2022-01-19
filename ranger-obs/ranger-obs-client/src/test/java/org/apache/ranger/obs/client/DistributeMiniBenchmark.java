/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 功能描述
 *
 * @since 2021-07-29
 */
public class DistributeMiniBenchmark implements Tool {

    private static final String USAGE =
        "Usage: " + DistributeMiniBenchmark.class.getSimpleName() +
            " [genericOptions]" +
            " -[concurrency]  |" +
            " -[requests]  |" +
            " -[mapper]  |" +
            " -[path]  |" +
            " -[action]  |" +
            " -[output]  |" +
            " -[runtime]  |" +
            " -[s]  |";

    private Configuration config;

    public DistributeMiniBenchmark() {
        this.config = new Configuration();
    }

    public static void main(String[] args) {
        DistributeMiniBenchmark bench = new DistributeMiniBenchmark();
        int res = -1;
        try {
            res = ToolRunner.run(bench, args);
        } catch (Exception e) {
            System.err.print(StringUtils.stringifyException(e));
            res = -2;
        }
        if (res == -1) {
            System.err.println(USAGE);
        }
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Missing arguments.");
            return -1;
        }

        int concurrency = 1;
        int requests = 1;
        String path = "";
        String result = "";
        String action = "read";
        long runtime = 0;
        long startline = 0;
        int numMapper = 1;
        boolean detail = false;
        boolean longrun = false;
        for (int i = 0; i < args.length; i++) { // parse command line
            if (StringUtils.toLowerCase(args[i]).startsWith("-concurrency")) {
                concurrency = Integer.parseInt(args[++i]);
            } else if (args[i].equalsIgnoreCase("-requests")) {
                requests = Integer.parseInt(args[++i]);
            } else if (args[i].equalsIgnoreCase("-mapper")) {
                numMapper = Integer.parseInt(args[++i]);
            } else if (args[i].equalsIgnoreCase("-path")) {
                path = args[++i];
            } else if (args[i].equalsIgnoreCase("-action")) {
                action = args[++i];
            } else if (args[i].equalsIgnoreCase("-output")) {
                result = args[++i];
            } else if (args[i].equalsIgnoreCase("-runtime")) {
                runtime = Long.parseLong(args[++i]);
            } else if (args[i].equalsIgnoreCase("-startline")) {
                startline = Long.parseLong(args[++i]);
            } else if (args[i].equalsIgnoreCase("-s")) {
                detail = true;
            } else if (args[i].equalsIgnoreCase("-l")) {
                longrun = true;
            } else {
                System.err.println("Illegal argument: " + args[i]);
                return -1;
            }
        }
        Configuration conf = getConf();
        conf.setInt("ranger.obs.bench.concurrency", concurrency);
        conf.setInt("ranger.obs.bench.requests", requests);
        conf.set("ranger.obs.bench.action", action);
        conf.set("ranger.obs.bench.path", path);
        conf.setLong("ranger.obs.bench.runtime", runtime);
        conf.setLong("ranger.obs.bench.startline", startline);
        conf.setBoolean("ranger.obs.bench.detail", detail);
        conf.setBoolean("ranger.obs.bench.longrun", longrun);
        conf.setInt(MRJobConfig.NUM_MAPS, numMapper);
        Job job = Job.getInstance(conf, "MiniBenchmark");

        job.setJarByClass(DistributeMiniBenchmark.class);

        job.setMapperClass(BenchMapper.class);
        job.setInputFormatClass(EmptyInputFormat.class);
        // job.setMapOutputKeyClass(IntWritable.class);
        // job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path("ignored"));

        job.setReducerClass(AccumulatingReducer.class);
        // job.setOutputFormatClass();
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Path outputDir = new Path(result);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setNumReduceTasks(1);

        job.setSpeculativeExecution(false);
        job.setJobName("MiniBenchmark job");
        job.waitForCompletion(true);
        return 0;
    }

    @Override
    public Configuration getConf() {
        return config;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.config = configuration;
    }
}

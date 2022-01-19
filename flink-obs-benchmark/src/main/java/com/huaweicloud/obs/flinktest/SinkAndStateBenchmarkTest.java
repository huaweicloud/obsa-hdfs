package com.huaweicloud.obs.flinktest;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能描述
 *
 * @since 2021-05-11
 */
public class SinkAndStateBenchmarkTest {

    private static final Logger LOG = LoggerFactory.getLogger(SinkAndStateBenchmarkTest.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        //sink,state,all
        String testCase = params.get("testCase", "sink");
        final String checkpointPath = params.getRequired("checkpointPath");
        final long checkPointinterval = params.getLong("checkPointinterval", 5000);

        String sinkPath = "";
        if (!"state".equals(testCase)) {
            sinkPath = params.getRequired("sinkPath");
        }
        int sinkParallelism = params.getInt("sinkParallelism", 1);
        final long rollSize = params.getLong("rollSize", 128 * 1024 * 1024);
        final long rollTime = params.getLong("rollTime", 60 * 1000);

        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        //checkpoint配置
        env.enableCheckpointing(checkPointinterval);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig()
            .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend
        env.setStateBackend(new FsStateBackend(checkpointPath));

        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource(params);
        DataStream<String> dataStreamSource = env.addSource(dataGeneratorSource).name("customSource");
        DataStream<String> map;
        if ("sink".equals(testCase)) {
            LOG.info("仅sink测试");
            map = dataStreamSource.map(new MapFunction<String, String>() {
                @Override
                public String map(String s) throws Exception {
                    return s;
                }
            });
        } else {
            map = dataStreamSource.map(new StateTestMap(params));
        }

        if ("state".equals(testCase)) {
            map.addSink(new DiscardingSink());
        } else {
            DefaultRollingPolicy rollingPolicy = DefaultRollingPolicy
                .create()
                .withMaxPartSize(rollSize) // 设置每个文件的最大大小 ,默认是128M
                .withRolloverInterval(rollTime) // 滚动写入新文件的时间，默认60s
                .withInactivityInterval(60 * 1000) // 60s空闲，就滚动写入新的文件
                .build();
            //obs://obsa-bigdata-posix/wgptest/data
            final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(sinkPath), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new BasePathBucketAssigner())
                .withRollingPolicy(rollingPolicy)
                .withBucketCheckInterval(1000L) // 桶检查间隔，这里设置为1s
                // .withOutputFileConfig()
                .build();

            //3.添加sink
            map.addSink(sink).name("StreamingFileSink").setParallelism(sinkParallelism);
        }

        env.execute(testCase);
    }
}

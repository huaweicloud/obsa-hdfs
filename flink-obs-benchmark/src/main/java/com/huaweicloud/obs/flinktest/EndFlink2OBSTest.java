package com.huaweicloud.obs.flinktest;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class EndFlink2OBSTest {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String kafkaServers = params.getRequired("kafkaServers");
        String kafkaTopic = params.get("kafkaTopic", "EndToEndTest");
        String kafkaGroup = params.get("kafkaGroup", "EndFlink2OBSTest");
        String checkPointPath = params.getRequired("checkPointPath");
        String sinkPath = params.getRequired("sinkPath");

        System.out.println("---start EndFlink2OBSTest---");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //checkpoint配置
        env.enableCheckpointing(5 * 1000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig()
            .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend
        env.setStateBackend(new FsStateBackend(checkPointPath));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaServers);
        properties.setProperty("group.id", kafkaGroup);
        properties.setProperty("auto.offset.reset", "earliest");
        FlinkKafkaConsumerBase<String> stringFlinkKafkaConsumerBase = new FlinkKafkaConsumer<>(kafkaTopic,
            new SimpleStringSchema(), properties);
        stringFlinkKafkaConsumerBase.setStartFromGroupOffsets();
        DataStream<String> stream = env.addSource(stringFlinkKafkaConsumerBase).setParallelism(1).uid("kafkac");

        DefaultRollingPolicy rollingPolicy = DefaultRollingPolicy
            .create()
            .withMaxPartSize(1024 * 1024) // 设置每个文件的最大大小 ,默认是128M。这里设置为120M
            .withRolloverInterval(60 * 1000) // 滚动写入新文件的时间，默认60s。这里设置为无限大
            .withInactivityInterval(60 * 1000) // 60s空闲，就滚动写入新的文件
            .build();

        final StreamingFileSink<String> sink = StreamingFileSink
            .forRowFormat(new Path(sinkPath), new SimpleStringEncoder<String>("UTF-8"))
            .withBucketAssigner(new BasePathBucketAssigner())
            .withRollingPolicy(rollingPolicy)
            .withBucketCheckInterval(1000L)// 桶检查间隔，这里设置为1s
            // .withOutputFileConfig()
            .build();
        stream.addSink(sink).name("OBSSinkTest").setParallelism(1);

        // stream.print().setParallelism(1);
        env.execute("EndFlink2OBSTest");

        System.out.println("end EndFlink2OBSTest");
    }

}
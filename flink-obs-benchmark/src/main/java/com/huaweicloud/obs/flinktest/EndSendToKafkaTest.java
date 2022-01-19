package com.huaweicloud.obs.flinktest;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 功能描述
 *
 * @since 2021-05-14
 */
public class EndSendToKafkaTest {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        long totalRecords = params.getLong("totalRecords", 120);
        String kafkaServers = params.getRequired("kafkaServers");
        String kafkaTopic = params.get("kafkaTopic", "EndToEndTest");
        String checkPointPath = params.getRequired("checkPointPath");

        System.out.println("start EndSendToKafkaTest");

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

        EndSequenceGenerator sequenceGenerator = new EndSequenceGenerator(1, totalRecords);
        EndDataGeneratorSource dataGeneratorSource = new EndDataGeneratorSource(sequenceGenerator, 1, totalRecords);
        DataStreamSource<String> dataStream = env.addSource(dataGeneratorSource).setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaServers);

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
            kafkaTopic,                  // 目标 topic
            new SimpleStringSchema(),     // 序列化 schema
            properties); // 容错

        dataStream.addSink(myProducer).name("kafkaSinkTest").setParallelism(1);

        env.execute("EndSendToKafkaTest");

        System.out.println("end EndSendToKafkaTest");
    }

}

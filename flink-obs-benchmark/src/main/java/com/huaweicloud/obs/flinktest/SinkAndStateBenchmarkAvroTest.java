package com.huaweicloud.obs.flinktest;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.avro.AvroBuilder;
import org.apache.flink.formats.avro.AvroWriterFactory;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 功能描述
 *
 * @since 2021-05-11
 */
public class SinkAndStateBenchmarkAvroTest {

    private static final Logger LOG = LoggerFactory.getLogger(SinkAndStateBenchmarkAvroTest.class);

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
        env.getCheckpointConfig().setCheckpointTimeout(300000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig()
            .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend
        env.setStateBackend(new FsStateBackend(checkpointPath));

        // final Configuration conf = new Configuration();
        // conf.setString("fs.obs.access.key", "xxx");
        // conf.setString("fs.obs.secret.key", "xxx");
        // conf.setString("fs.obs.endpoint", "xxx");
        // FileSystem.initialize(conf);

        AvroDataGeneratorSource dataGeneratorSource = new AvroDataGeneratorSource(params);
        DataStream<BaseEvent> dataStreamSource = env.addSource(dataGeneratorSource).name("customSource");
        DataStream<BaseEvent> map = null;
        if ("sink".equals(testCase)) {
            LOG.info("仅sink测试");
             map = dataStreamSource.map(new MapFunction<BaseEvent, BaseEvent>() {
                @Override
                public BaseEvent map(BaseEvent value) throws Exception {
                    return value;
                }
            });
        } else {
            map = dataStreamSource.map(new AvroStateTestMap(params));
        }

        if ("state".equals(testCase)) {
            map.addSink(new DiscardingSink<>());
        } else {

            StreamingFileSink sink = StreamingFileSink
                .forBulkFormat(
                    new Path(sinkPath),
                    new AvroWriterFactory<>(new AvroBuilder() {
                        @Override
                        public DataFileWriter createWriter(OutputStream outputStream) throws IOException {
                            Schema schema = ReflectData.get().getSchema(BaseEvent.class);
                            ReflectDatumWriter<Object> datumWriter = new ReflectDatumWriter<>(schema);
                            DataFileWriter<Object> dataFileWriter = new DataFileWriter<>(datumWriter);
                            dataFileWriter.create(schema, outputStream);
                            // dataFileWriter.setFlushOnEveryBlock(false);
                            // dataFileWriter.setSyncInterval()
                            return dataFileWriter;
                        }
                    })
                )
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new BucketAssigner<BaseEvent, String>() {
                    @Override
                    public String getBucketId(BaseEvent baseEvent, Context context) {
                        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH")
                            .withZone(ZoneId.of("Asia/Shanghai"));
                        return String.format("event_%s/dhour=%s", baseEvent.getProject_id(),
                            dateTimeFormatter.format(Instant.ofEpochMilli(context.currentProcessingTime())));
                    }
                    @Override
                    public SimpleVersionedSerializer<String> getSerializer() {
                        return SimpleVersionedStringSerializer.INSTANCE;
                    }
                })
                .build();




            // StreamingFileSink sink = StreamingFileSink
            //     .forBulkFormat(
            //         new Path(sinkPath),
            //         new AvroWriterFactory<>(new AvroBuilder() {
            //             @Override
            //             public DataFileWriter createWriter(OutputStream outputStream) throws IOException {
            //                 Schema schema = ReflectData.get().getSchema(BaseEvent.class);
            //                 ReflectDatumWriter<Object> datumWriter = new ReflectDatumWriter<>(schema);
            //                 DataFileWriter<Object> dataFileWriter = new DataFileWriter<>(datumWriter);
            //                 dataFileWriter.create(schema, outputStream);
            //                 // dataFileWriter.setFlushOnEveryBlock(false);
            //                 return dataFileWriter;
            //             }
            //         })
            //     )
            //     .withRollingPolicy(OnCheckpointRollingPolicy.build())
            //     .withBucketAssigner(new BasePathBucketAssigner())
            //     .build();

            //3.添加sink
            map.addSink(sink).name("StreamingFileSink").setParallelism(sinkParallelism);
        }

        env.execute(testCase);
    }
}

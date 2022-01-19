package com.huaweicloud.obs.flinktest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.UnsupportedEncodingException;

/**
 * 功能描述
 *
 * @since 2021-05-12
 */
public class SequenceDataGenerator {
    static public Tuple2<LongWritable, Text> getRecord(int recordLength) throws UnsupportedEncodingException {
        // long id, String name, String age, boolean sex, String parent, String project, String fee, String cost_time
        Tuple2<LongWritable, Text> longWritableTextTuple2 = new Tuple2<LongWritable, Text>(new LongWritable(123456789),new Text("testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest"));
        return longWritableTextTuple2;
    }
}

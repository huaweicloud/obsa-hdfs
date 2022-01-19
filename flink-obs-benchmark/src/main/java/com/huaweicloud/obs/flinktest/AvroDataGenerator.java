package com.huaweicloud.obs.flinktest;

import java.io.UnsupportedEncodingException;

/**
 * 功能描述
 *
 * @since 2021-05-12
 */
public class AvroDataGenerator {
    static public BaseEvent getRecord(int recordLength) throws UnsupportedEncodingException {
        // long id, String name, String age, boolean sex, String parent, String project, String fee, String cost_time
        return new BaseEvent(123456789,
            "name",
            "age",
            false,
            "parent",
            "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest",
            "fee",
            "cost_time"
        );
    }
}

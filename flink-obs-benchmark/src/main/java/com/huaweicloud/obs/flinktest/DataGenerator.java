package com.huaweicloud.obs.flinktest;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * 功能描述
 *
 * @since 2021-05-12
 */
public class DataGenerator {
    static public String getRecord(int recordLength) throws UnsupportedEncodingException {
        // String line = "ddd";
        byte[] line = "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest"
            .getBytes("UTF-8");
        byte[] valueByte;
        valueByte = new byte[recordLength];
        if (line.length < recordLength) {
            // There is no enough content in line, fill rest space with 0
            System.arraycopy(line, 0, valueByte, 0, line.length);
            Arrays.fill(valueByte, line.length, recordLength, (byte) 0);
        } else {
            System.arraycopy(line, 0, valueByte, 0, recordLength);
        }

        return new String(valueByte, "UTF-8");
    }
}

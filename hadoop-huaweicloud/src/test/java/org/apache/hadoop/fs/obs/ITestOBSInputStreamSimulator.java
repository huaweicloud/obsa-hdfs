package org.apache.hadoop.fs.obs;

import org.apache.hadoop.fs.obs.memartscc.OBSInputStreamSimulator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ITestOBSInputStreamSimulator {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testRead() throws Exception{
        long fileStatusLength = 100 * 1024 * 1024;
        long readAheadRangeValue = 1024 * 1024;
        OBSInputStreamSimulator obsInputStreamSimulator =
                new OBSInputStreamSimulator(fileStatusLength, readAheadRangeValue);
        int willReadBytes;
        long readBytesFromOBS;

        willReadBytes = 5 *1024 * 1024;
        readBytesFromOBS = obsInputStreamSimulator.read(willReadBytes);
        assertEquals("First read", willReadBytes, readBytesFromOBS);

        willReadBytes = 512 * 1024;
        readBytesFromOBS = obsInputStreamSimulator.read(willReadBytes);
        assertEquals("Second read", readBytesFromOBS, readAheadRangeValue);

        readBytesFromOBS = obsInputStreamSimulator.read(willReadBytes);
        assertEquals("Second read", readBytesFromOBS, 0);

        obsInputStreamSimulator.close();
    }
}


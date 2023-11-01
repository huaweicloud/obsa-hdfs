/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.obs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;

/**
 * OBS tests for configuring block size.
 */
@RunWith(Parameterized.class)
public class ITestOBSHFlush {
    private OBSFileSystem fs;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private static final Logger LOG = LoggerFactory.getLogger(
        ITestOBSHFlush.class);

    @Parameterized.Parameters
    public static Collection inputStreams() {
        return Arrays.asList(
            OBSConstants.OUTPUT_STREAM_HFLUSH_POLICY_SYNC,
            OBSConstants.OUTPUT_STREAM_HFLUSH_POLICY_EMPTY,
            OBSConstants.OUTPUT_STREAM_HFLUSH_POLICY_FLUSH
        );
    }

    private boolean downgrade;

    public ITestOBSHFlush(String downgradePolicy) throws IOException {
        Configuration conf = OBSContract.getConfiguration(null);
        conf.set(OBSConstants.MULTIPART_SIZE, String.valueOf(5 * 1024 * 1024));
        conf.set(OBSConstants.OUTPUT_STREAM_HFLUSH_POLICY, downgradePolicy);
        fs = OBSTestUtils.createTestFileSystem(conf);
        if (fs.exists(getTestPath("testFlush"))) {
            fs.delete(getTestPath("testFlush"), true);
        }
        downgrade = (!downgradePolicy.equals(OBSConstants.OUTPUT_STREAM_HFLUSH_POLICY_SYNC));
    }

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
            fs.close();
            fs = null;
        }
    }

    private Path getTestPath(String relativePath) {
        return new Path(testRootPath + "/" + relativePath);
    }

    @Test
    // 200M数据，分10次写，每次写完hflush，校验文件大小
    public void testFlush01() {
        if (!fs.isFsBucket()) {
            return;
        }
        try {
            doTheJob("testFlush", OBSAppendTestUtil.BLOCK_SIZE, (short) 2, false, downgrade,
                EnumSet.noneOf(HdfsDataOutputStream.SyncFlag.class),
                1024 * 1024 * 200);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            assertNull("cache exception", e);
        }
    }

    @Test
    // 20M数据，分10次写，每次写完hflush，校验文件大小
    public void testFlush02() {
        if (!fs.isFsBucket()) {
            return;
        }
        try {
            doTheJob("testFlush", OBSAppendTestUtil.BLOCK_SIZE, (short) 2, false, downgrade,
                EnumSet.noneOf(HdfsDataOutputStream.SyncFlag.class),
                1024 * 1024 * 20);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            assertNull("cache exception", e);
        }
    }

    @Test
    // 51M数据，分10次写，每次写完hflush，校验文件大小
    public void testFlush03() {
        if (!fs.isFsBucket()) {
            return;
        }
        try {
            doTheJob("testFlush", OBSAppendTestUtil.BLOCK_SIZE, (short) 2, false, downgrade,
                EnumSet.noneOf(HdfsDataOutputStream.SyncFlag.class),
                1024 * 1024 * 51);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            assertNull("cache exception", e);
        }
    }

    /**
     * The method starts new cluster with defined Configuration; creates a file
     * with specified block_size and writes 10 equal sections in it; it also
     * calls hflush/hsync after each write and throws an IOException in case of
     * an error.
     *
     * @param fileName   of the file to be created and processed as required
     * @param block_size value to be used for the file's creation
     * @param replicas   is the number of replicas
     * @param isSync     hsync or hflush
     * @param syncFlags  specify the semantic of the sync/flush
     * @throws IOException in case of any errors
     */
    public void doTheJob(final String fileName,
        long block_size, short replicas, boolean isSync, boolean downgrade,
        EnumSet<HdfsDataOutputStream.SyncFlag> syncFlags, int size)
        throws IOException {
        byte[] fileContent;
        final int SECTIONS = 10;

        fileContent = OBSAppendTestUtil.initBuffer(size);

        FSDataInputStream is;
        try {
            Path path = getTestPath(fileName);
            FSDataOutputStream stm = fs.create(path, false, 4096, replicas,
                block_size);

            int tenth = size / SECTIONS;
            int rounding = size - tenth * SECTIONS;
            for (int i = 0; i < SECTIONS; i++) {
                // write to the file
                stm.write(fileContent, tenth * i, tenth);

                // Wait while hflush/hsync pushes all packets through built pipeline
                if (isSync) {
                    ((OBSBlockOutputStream) stm.getWrappedStream()).hsync();
                } else {
                    ((OBSBlockOutputStream) stm.getWrappedStream()).hflush();
                }

                if (downgrade) {
                    // skip check flushed block
                    continue;
                }

                // Check file length if update length is required
                if (isSync && syncFlags.contains(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH)) {
                    long currentFileLength = fs.getFileStatus(path).getLen();
                    assertEquals("File size doesn't match for hsync/hflush with updating the length", tenth * (i + 1), currentFileLength);
                } else if (!isSync || (!syncFlags.contains(HdfsDataOutputStream.SyncFlag.END_BLOCK)
                    && !syncFlags.contains(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH))) {
                    long currentFileLength = fs.getFileStatus(path).getLen();
                    assertEquals("File size doesn't match for hsync/hflush with updating the length", tenth * (i + 1), currentFileLength);
                }

                byte[] toRead = new byte[tenth];
                byte[] expected = new byte[tenth];
                System.arraycopy(fileContent, tenth * i, expected, 0, tenth);
                // Open the same file for read. Need to create new reader after every write operation(!)
                is = fs.open(path);
                is.seek(tenth * i);
                int readBytes = is.read(toRead, 0, tenth);
                System.out.println("Has read " + readBytes);
                Assert.assertTrue("Should've get more bytes",
                    (readBytes > 0) && (readBytes <= tenth));
                is.close();
                checkData(toRead, 0, readBytes, expected,
                    "Partial verification");
            }

            stm.write(fileContent, tenth * SECTIONS, rounding);
            stm.close();

            OBSAppendTestUtil.checkFullFile(fs, path, fileContent.length,
                fileContent, "hflush()");
        } finally {
            fs.delete(new Path(testRootPath), true);
            fs.close();
            fs = null;
        }
    }

    void checkData(final byte[] actual, int from, int len,
        final byte[] expected, String message) {
        for (int idx = 0; idx < len; idx++) {
            assertEquals(
                message + " byte " + (from + idx) + " differs. expected " +
                    expected[from + idx] + " actual " + actual[idx],
                expected[from + idx], actual[idx]);
            actual[idx] = 0;
        }
    }
}

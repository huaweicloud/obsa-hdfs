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

import static org.apache.hadoop.fs.obs.OBSConstants.FAST_UPLOAD_BYTEBUFFER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.security.AccessControlException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * OBS tests for configuring block size.
 */
public class ITestOBSGetContentSummary {
    private OBSFileSystem fs;

    private Configuration conf;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private static final String TEST_PREFIX = "test_du/";

    private static final byte[] dataSet = ContractTestUtils.dataset(16, 0, 10);

    private static final Logger LOG =
        LoggerFactory.getLogger(ITestOBSGetContentSummary.class);

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        conf = OBSContract.getConfiguration(null);
    }

    private void init(boolean contentEnable, boolean fsDFSListEnable)
        throws IOException {
        conf.setClass(OBSConstants.OBS_METRICS_CONSUMER,
            MockMetricsConsumer.class, BasicMetricsConsumer.class);
        conf.setBoolean(OBSConstants.METRICS_SWITCH, true);
        conf.setBoolean(OBSConstants.OBS_CONTENT_SUMMARY_ENABLE, contentEnable);
        conf.setBoolean(OBSConstants.OBS_CLIENT_DFS_LIST_ENABLE, fsDFSListEnable);
        conf.set(OBSConstants.FAST_UPLOAD_BUFFER, FAST_UPLOAD_BYTEBUFFER);
        fs = OBSTestUtils.createTestFileSystem(conf);
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }

    private Path getTestPath(String testPath) {
        return new Path(testRootPath + "/" + testPath);
    }

    @Test
    // 计算对象桶一级目录的ContentSummary，校验正确性
    public void testObjectBucketOnePath() throws Exception {
        testGetContentSummaryOnePath(TEST_PREFIX, false);
    }

    @Test
    // 计算对象桶子目录的ContentSummary，校验正确性
    public void testObjectBucketSubPath() throws Exception {
        testGetContentSummarySubPath(TEST_PREFIX, false);
    }

    @Test
    // 计算文件桶一级目录的ContentSummary，校验正确性
    public void testPosixBucketOnePath() throws Exception {
        testGetContentSummaryOnePath(TEST_PREFIX, true);
    }

    @Test
    // 计算文件桶子目录的ContentSummary，校验正确性
    public void testPosixBucketSubPath() throws Exception {
        testGetContentSummarySubPath(TEST_PREFIX, true);
    }

    @Test
    // 测试空文件的ContentSummary，DirectoryCount为0，FileCount为1，Length为0
    public void testGetContentSummaryOfFile01() throws Exception {
        init(true, true);
        Path testFile = getTestPath("test_file");
        fs.delete(testFile, true);

        FSDataOutputStream outputStream = fs.create(testFile, false);
        outputStream.close();

        ContentSummary summary = fs.getContentSummary(testFile);
        assertTrue(summary.getDirectoryCount() == 0);
        assertTrue(summary.getFileCount() == 1);
        assertTrue(summary.getLength() == 0);
        fs.delete(testFile, true);
    }

    @Test
    // 测试非空文件的ContentSummary，DirectoryCount为0，FileCount为1，Length为文件长度
    public void testGetContentSummaryOfFile02() throws Exception {
        init(true, true);
        Path testFile = getTestPath("test_file");
        fs.delete(testFile, true);

        FSDataOutputStream outputStream = fs.create(testFile, false);
        byte[] data = {1, 2, 3, 4, 5};
        outputStream.write(data);
        outputStream.close();

        ContentSummary summary = fs.getContentSummary(testFile);
        assertTrue(summary.getDirectoryCount() == 0);
        assertTrue(summary.getFileCount() == 1);
        assertTrue(summary.getLength() == data.length);
        fs.delete(testFile, true);
    }

    @Test
    // 路径不存在，抛FileNotFoundException
    public void testGetContentSummaryAbnormal01() throws Exception {
        init(true, true);
        Path testFile = getTestPath("test_file");
        fs.delete(testFile, true);

        boolean hasException = false;
        try {
            fs.getContentSummary(testFile);
        } catch (FileNotFoundException e) {
            hasException = true;
        }
        assertTrue(hasException);
    }

    @Test
    // 路径的父目录及上级目录不存在，抛FileNotFoundException
    public void testGetContentSummaryAbnormal02() throws Exception {
        init(true, true);
        Path testFile = getTestPath("a001/b001/test_file");
        fs.delete(testFile.getParent().getParent(), true);

        assertFalse(fs.exists(testFile.getParent()));

        boolean hasException = false;
        try {
            fs.getContentSummary(testFile);
        } catch (FileNotFoundException e) {
            hasException = true;
        }
        assertTrue(hasException);
    }

    @Test
    // 路径的父目录及上级目录非目录，抛AccessControlException
    public void testGetContentSummaryAbnormal03() throws Exception {
        init(true, true);
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFile = getTestPath("a001/b001/test_file");
        fs.delete(testFile.getParent().getParent(), true);

        FSDataOutputStream outputStream = fs.create(testFile.getParent(),
            false);
        outputStream.close();

        boolean hasException = false;
        try {
            fs.getContentSummary(testFile);
        } catch (AccessControlException e) {
            hasException = true;
        }
        assertTrue(hasException);
    }

    private void constructTestPaths(String testDir, int dirDeep, int subDirNum,
        int subFileNum) throws Exception {
        if (dirDeep <= 0) {
            return;
        }
        if (!testDir.endsWith("/")) {
            testDir = testDir + "/";
        }
        boolean rslt = fs.mkdirs(getTestPath(testDir));
        Assert.assertTrue(rslt);

        for (int i = 0; i < subFileNum; i++) {
            FSDataOutputStream outputStream = fs.create(
                getTestPath(testDir + "-subFile-" + i), true);
            outputStream.write(dataSet);
            outputStream.close();
        }

        for (int i = 0; i < subDirNum; i++) {
            constructTestPaths(testDir + "-" + dirDeep + "-" + i + "/",
                dirDeep - 1, subDirNum, subFileNum);
        }
    }

    private void testGetContentSummaryOnePath(String prefix,
        boolean isPosixTest) throws Exception {
        // test primitive
        ContentSummary primitive, optimization, optimizationWithDFS = null;
        long delay1, delay2, delay3;
        try {
            init(false, false);
            if (isPosixTest != fs.isFsBucket()) {
                return;
            }
            fs.delete(getTestPath(prefix), true);
            constructTestPaths(prefix, 3, 5, 5);
            long startTime = System.nanoTime();
            primitive = fs.getContentSummary(getTestPath(prefix));
            delay1 = System.nanoTime() - startTime;
        } finally {
            fs.close();
            fs = null;
        }

        try {
            init(true, false);
            long startTime = System.nanoTime();
            optimization = fs.getContentSummary(getTestPath(prefix));
            delay2 = System.nanoTime() - startTime;
        } finally {
            fs.close();
            fs = null;
        }

        // test optimization
        try {
            init(true, true);
            long startTime = System.nanoTime();
            optimizationWithDFS = fs.getContentSummary(getTestPath(prefix));
            delay3 = System.nanoTime() - startTime;
            assertTrue(fs.delete(getTestPath(prefix), true));
        } finally {
            if (fs != null) {
                fs.close();
                fs = null;
            }
        }

        LOG.info("delay (ns): {} vs. {} vs. {}", delay1, delay2, delay3);

        // compare
        assertTrue(compareContentSummary(primitive, optimization));
        assertTrue(compareContentSummary(primitive, optimizationWithDFS));
    }

    private void testGetContentSummarySubPath(String prefix,
        boolean isPosixTest) throws Exception {
        // test primitive
        ContentSummary primitive = null;
        Map<String, ContentSummary> summarys = new HashMap<>();
        try {
            init(false, false);
            if (isPosixTest != fs.isFsBucket()) {
                return;
            }
            fs.delete(getTestPath(prefix), true);
            constructTestPaths(prefix, 3, 5, 5);
            FileStatus[] status = fs.listStatus(getTestPath(prefix));
            long length = 0;
            long fileCount = 0;
            long dirCount = 0;
            for (FileStatus s : status) {
                ContentSummary summary = fs.getContentSummary(s.getPath());
                summarys.put(s.getPath().toString(), summary);
                length += summary.getLength();
                fileCount += summary.getFileCount();
                dirCount += summary.getDirectoryCount();
            }
            primitive = new ContentSummary(length, fileCount, dirCount);
        } finally {
            if (fs != null) {
                fs.close();
                fs = null;
            }
        }

        // test optimization
        ContentSummary optimization = null;
        Map<String, ContentSummary> optSummarys = new HashMap<>();
        try {
            init(true, true);
            FileStatus[] status = fs.listStatus(getTestPath(prefix));
            long length = 0;
            long fileCount = 0;
            long dirCount = 0;
            for (FileStatus s : status) {
                ContentSummary summary = fs.getContentSummary(s.getPath());
                optSummarys.put(s.getPath().toString(), summary);
                length += summary.getLength();
                fileCount += summary.getFileCount();
                dirCount += summary.getDirectoryCount();
            }
            optimization = new ContentSummary(length, fileCount, dirCount);
            assertTrue(fs.delete(getTestPath(prefix), true));
        } finally {
            if (fs != null) {
                fs.close();
                fs = null;
            }
        }

        // compare
        boolean isSameResult = compareContentSummary(primitive, optimization);
        assertTrue(isSameResult);
    }

    private boolean compareContentSummary(ContentSummary s1,
        ContentSummary s2) {
        LOG.info(String.format("s1{len[%s], fileCount[%s], dirCount[%s]",
            s1.getLength(), s1.getFileCount(), s1.getDirectoryCount()));

        LOG.info(String.format("s2{len[%s], fileCount[%s], dirCount[%s]",
            s2.getLength(), s2.getFileCount(), s2.getDirectoryCount()));

        if (s1.getLength() != s2.getLength()
            || s1.getDirectoryCount() != s2.getDirectoryCount()
            || s1.getFileCount() != s2.getFileCount()) {
            return false;
        }
        return true;
    }
}

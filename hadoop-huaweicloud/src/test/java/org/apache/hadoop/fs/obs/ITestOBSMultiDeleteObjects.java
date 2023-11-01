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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

public class ITestOBSMultiDeleteObjects {
    private OBSFileSystem fs;

    private int testBufferSize;

    private int modulus;

    private byte[] testBuffer;

    private int maxEntriesToDelete;

    private static final int DEFAULT_MULTI_DELETE_THRESHOLD = 3;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        conf.setInt(OBSConstants.MULTI_DELETE_MAX_NUMBER, 1000);
        maxEntriesToDelete = 1000;
        fs = OBSTestUtils.createTestFileSystem(conf);
        testBufferSize = fs.getConf().getInt(
            ContractTestUtils.IO_CHUNK_BUFFER_SIZE, 128);
        modulus = fs.getConf().getInt(
            ContractTestUtils.IO_CHUNK_MODULUS_SIZE, 128);
        testBuffer = new byte[testBufferSize];

        for (int i = 0; i < testBuffer.length; ++i) {
            testBuffer[i] = (byte) (i % modulus);
        }
    }

    private Path getTestPath(String testPath) {
        return new Path(testRootPath + "/" + testPath);
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }

    private void constructTestData(Path path, int subDirNum, int childFileNum)
        throws IOException {
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        ContractTestUtils.assertPathDoesNotExist(fs,
            path.toString() + "should not exist.", path);

        fs.mkdirs(path);
        String directory = path.toString();
        if (!directory.endsWith("/")) {
            directory = directory + "/";
        }

        for (int i = 0; i < subDirNum; i++) {
            fs.mkdirs(new Path(directory + "subDir-" + i));
        }

        for (int j = 0; j < childFileNum; j++) {
            FSDataOutputStream outputStream = fs.create(
                new Path(directory + "childFile-" + j));
            outputStream.write(testBuffer);
            outputStream.close();
        }
    }

    private long testMultiDelete(String testPathStr, int subDirNum,
        int childFileNum) throws IOException {
        Path testPath = getTestPath(testPathStr);
        constructTestData(testPath, subDirNum, childFileNum);
        long startTime = System.nanoTime();
        fs.delete(testPath, true);
        long endTime = System.nanoTime();
        ContractTestUtils.assertPathDoesNotExist(fs,
            testPath.toString() + " should not exist.", testPath);

        return (endTime - startTime) / 1000;
    }

    @Test
    // 一级目录包含10个子目录，批删一级目录
    public void testMultiDelete001() throws IOException {
        int dirNum = maxEntriesToDelete / 100;
        int fileNum = 0;
        testMultiDelete("testMultiDelete-" + dirNum + "-" + fileNum, dirNum,
            fileNum);
    }

    @Test
    // 一级目录包含10个子目录，每个目录包含10个子文件，批删一级目录
    public void testMultiDelete002() throws IOException {
        int dirNum = maxEntriesToDelete / 100;
        int fileNum = dirNum;
        testMultiDelete("testMultiDelete-" + dirNum + "-" + fileNum, dirNum,
            fileNum);
    }

    @Test
    // 一级目录包含100个子目录，批删一级目录
    public void testMultiDelete003() throws IOException {
        int dirNum = maxEntriesToDelete / 10;
        int fileNum = 0;
        testMultiDelete("testMultiDelete-" + dirNum + "-" + fileNum, dirNum,
            fileNum);
    }

    @Test
    // 一级目录包含100个子目录，每个目录包含100个子文件，批删一级目录
    public void testMultiDelete004() throws IOException {
        int dirNum = maxEntriesToDelete / 10;
        int fileNum = dirNum;
        testMultiDelete("testMultiDelete-" + dirNum + "-" + fileNum, dirNum,
            fileNum);
    }

    @Test
    // 一级目录包含1000个子目录，批删一级目录
    public void testMultiDelete005() throws IOException {
        int dirNum = maxEntriesToDelete;
        int fileNum = 0;
        testMultiDelete("testMultiDelete-" + dirNum + "-" + fileNum, dirNum,
            fileNum);
    }

    @Test
    // 一级目录包含1010个子目录，每个目录包含10个子文件，批删一级目录
    public void testMultiDelete006() throws IOException {
        int dirNum = maxEntriesToDelete + maxEntriesToDelete / 100;
        int fileNum = maxEntriesToDelete / 100;
        testMultiDelete("testMultiDelete-" + dirNum + "-" + fileNum, dirNum,
            fileNum);
    }

    @Test
    // 一级目录包含2000个子目录，批删一级目录
    public void testMultiDelete007() throws IOException {
        int dirNum = 2 * maxEntriesToDelete;
        int fileNum = 0;
        testMultiDelete("testMultiDelete-" + dirNum + "-" + fileNum, dirNum,
            fileNum);
    }

    @Test
    // 一级目录包含2010个子目录，每个目录包含10个子文件，批删一级目录
    public void testMultiDelete008() throws IOException {
        int dirNum = 2 * maxEntriesToDelete + maxEntriesToDelete / 100;
        int fileNum = maxEntriesToDelete / 100;
        testMultiDelete("testMultiDelete-" + dirNum + "-" + fileNum, dirNum,
            fileNum);
    }

    @Test
    // 不同批删门禁时，一级目录包含3个子目录，每个目录包含3个子文件，删除一级目录时间对比
    public void testMultiDeleteThreshold() throws IOException {
        Configuration conf = fs.getConf();
        int defaultThreshold = conf.getInt(OBSConstants.MULTI_DELETE_THRESHOLD,
            DEFAULT_MULTI_DELETE_THRESHOLD);
        conf.setInt(OBSConstants.MULTI_DELETE_THRESHOLD, 2 * defaultThreshold);

        int dirNum = defaultThreshold - 1;
        int fileNum = defaultThreshold - 1;
        long timeUsedDefaultThreshold = testMultiDelete(
            "testMultiDeleteThreshold-" + dirNum + "-" + fileNum,
            dirNum, fileNum);

        conf.setInt(OBSConstants.MULTI_DELETE_THRESHOLD, 4 * defaultThreshold);
        long timeUsedThreshold = testMultiDelete(
            "testMultiDeleteThreshold-" + dirNum + "-" + fileNum,
            dirNum, fileNum);

        System.out.println(
            String.format(
                "double default threshold: %d, timeUsed(us): %s; threshold: %d, timeUsed(us): %s.",
                2 * defaultThreshold, timeUsedDefaultThreshold,
                4 * defaultThreshold, timeUsedThreshold));
        //        assertTrue(timeUsedThreshold < timeUsedDefaultThreshold);
    }
}
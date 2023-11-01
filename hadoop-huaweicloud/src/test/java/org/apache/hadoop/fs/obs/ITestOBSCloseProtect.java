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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class ITestOBSCloseProtect {
    private static final Logger LOG = LoggerFactory.getLogger(
            ITestOBSRename.class);
    private OBSFileSystem fs;

    private static String testRootPath = OBSTestUtils.generateUniqueTestPath();

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        final int partSize = 5 * 1024 * 1024;
        conf.set(OBSConstants.MULTIPART_SIZE, String.valueOf(partSize));
        conf.setClass(OBSConstants.OBS_METRICS_CONSUMER,
                MockMetricsConsumer.class, BasicMetricsConsumer.class);
        conf.setBoolean(OBSConstants.METRICS_SWITCH, true);
        fs = OBSTestUtils.createTestFileSystem(conf);
        if (fs.exists(new Path(testRootPath))) {
            fs.delete(new Path(testRootPath), true);
        }
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    // fs关闭后再执行open操作，抛出IOException
    public void testOpenAfterFSClose() throws IOException {
        String fileName = "file";
        Path filePath = new Path(testRootPath + "/" + fileName);

        fs.close();

        boolean closeException = false;
        try {
            fs.open(filePath);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            fs.open(filePath, 100);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行create操作，抛出IOException
    public void testCreateAfterFSClose() throws IOException {
        String fileName = "file";
        Path filePath = new Path(testRootPath + "/" + fileName);

        fs.close();

        boolean closeException = false;
        try {
            fs.create(filePath, new FsPermission((short) 00644), false, 4096,
                (short) 3, 128 * 1024 * 1024, null);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            fs.create(filePath, new FsPermission((short) 00644), null, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            fs.create(filePath);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            fs.create(filePath, null);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            fs.create(filePath, true);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            fs.create(filePath, (short) 3);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            fs.create(filePath, (short) 3, null);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            fs.create(filePath, true, 4096);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            fs.create(filePath, true, 4096, null);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            fs.create(filePath, true, 4096, (short) 3,
                128 * 1024 * 1024);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            fs.create(filePath, true, 4096, (short) 3,
                128 * 1024 * 1024, null);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行CreateNonRecursive操作，抛出IOException
    public void testCreateNonRecursiveAfterFSClose() throws IOException {
        String fileName = "file";
        Path filePath = new Path(testRootPath + "/" + fileName);

        fs.close();

        boolean closeException = false;
        try {
            fs.createNonRecursive(filePath, new FsPermission((short) 00644),
                true, 4096, (short) 3, 128 * 1024 * 1024, null);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            fs.createNonRecursive(filePath, new FsPermission((short) 00644),
                null, 4096, (short) 3, 128 * 1024 * 1024, null);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行append操作，抛出IOException
    public void testAppendAfterFSClose() throws IOException {
        String fileName = "file";
        Path filePath = new Path(testRootPath + "/" + fileName);

        fs.close();

        boolean closeException = false;
        try {
            fs.append(filePath, 4096, null);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            fs.append(filePath);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行truncate操作，抛出IOException
    public void testTruncateAfterFSClose() throws IOException {
        String fileName = "file";
        Path filePath = new Path(testRootPath + "/" + fileName);

        fs.close();

        boolean closeException = false;
        try {
            fs.truncate(filePath, 4096);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行exists操作，抛出IOException
    public void testExistsAfterFSClose() throws IOException {
        String fileName = "file";
        Path filePath = new Path(testRootPath + "/" + fileName);

        fs.close();

        boolean closeException = false;
        try {
            fs.exists(filePath);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行rename操作，抛出IOException
    public void testRenameAfterFSClose() throws IOException {
        String fileNameFrom = "fileFrom";
        Path filePathFrom = new Path(
            testRootPath + "/" + fileNameFrom);
        String fileNameTo = "fileTo";
        Path filePathTo = new Path(
            testRootPath + "/" + fileNameTo);

        fs.close();

        boolean closeException = false;
        try {
            fs.rename(filePathFrom, filePathTo);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行delete操作，抛出IOException
    public void testDeleteAfterFSClose() throws IOException {
        String dirName = "dir";
        Path dirPath = new Path(testRootPath + "/" + dirName);

        fs.close();

        boolean closeException = false;
        try {
            fs.delete(dirPath, true);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行ListStatus操作，抛出IOException
    public void testListStatusAfterFSClose() throws IOException {
        String dirName = "dir";
        Path dirPath = new Path(testRootPath + "/" + dirName);

        fs.close();

        boolean closeException = false;
        try {
            fs.listStatus(dirPath, true);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            fs.listStatus(dirPath);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行Mkdirs操作，抛出IOException
    public void testMkdirsAfterFSClose() throws IOException {
        String dirName = "dir";
        Path dirPath = new Path(testRootPath + "/" + dirName);

        fs.close();

        boolean closeException = false;
        try {
            fs.mkdirs(dirPath, new FsPermission((short) 00644));
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行GetFileStatus操作，抛出IOException
    public void testGetFileStatusAfterFSClose() throws IOException {
        String fileName = "file";
        Path filePath = new Path(testRootPath + "/" + fileName);

        fs.close();

        boolean closeException = false;
        try {
            fs.getFileStatus(filePath);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行GetContentSummary操作，抛出IOException
    public void testGetContentSummaryAfterFSClose() throws IOException {
        String dirName = "dir";
        Path dirPath = new Path(testRootPath + "/" + dirName);

        fs.close();

        boolean closeException = false;
        try {
            fs.getContentSummary(dirPath);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行CopyFromLocalFile操作，抛出IOException
    public void testCopyFromLocalFileAfterFSClose() throws IOException {
        String fileNameLocal = "fileLocal";
        Path filePathLocal = new Path("/usr/" + fileNameLocal);
        String fileNameRemote = "fileRemote";
        Path filePathRemote = new Path(
            testRootPath + "/" + fileNameRemote);

        fs.close();

        boolean closeException = false;
        try {
            fs.copyFromLocalFile(true, true, filePathLocal, filePathRemote);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行ListFiles操作，抛出IOException
    public void testListFilesAfterFSClose() throws IOException {
        String dirName = "dir";
        Path dirPath = new Path(testRootPath + "/" + dirName);

        fs.close();

        boolean closeException = false;
        try {
            fs.listFiles(dirPath, true);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行ListLocatedStatus操作，抛出IOException
    public void testListLocatedStatusAfterFSClose() throws IOException {
        String dirName = "dir";
        Path dirPath = new Path(testRootPath + "/" + dirName);

        fs.close();

        boolean closeException = false;
        try {
            fs.listLocatedStatus(dirPath);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行InputStream相关操作，抛出IOException
    public void testInputStreamAfterFSClose() throws Exception {
        final int bufLen = 256 * 1024;
        final int sizeMB = 10;
        String fileName = "readTestFile_" + sizeMB + ".txt";
        Path readTestFile = new Path(
            testRootPath + "/" + fileName);
        long size = sizeMB * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, readTestFile, size, 256,
            255);

        FSDataInputStream instream = this.fs.open(readTestFile);

        fs.close();

        boolean closeException = false;
        try {
            instream.getPos();
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            instream.seek(1024);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            instream.seekToNewSource(1024);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            instream.read();
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bufLen);
            instream.read(byteBuffer);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            byte[] buf = new byte[1024];
            instream.read(buf, 0, 1024);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            instream.close();
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            instream.available();
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            byte[] buf = new byte[1024];
            instream.readFully(1024, buf, 0, 1024);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                if (fs.getMetricSwitch()) {
                    MockMetricsConsumer mmc
                            = (MockMetricsConsumer) fs.getMetricsConsumer();
                    assertEquals(BasicMetricsConsumer.MetricKind.abnormal, mmc.getMr().getKind());
                    LOG.warn("metricInfo:"+mmc.getMr().getExceptionIns());
                }
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            byte[] buf = new byte[1024];
            instream.read(1024, buf, 0, 1024);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            instream.setReadahead(1024L);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // fs关闭后再执行OutputStream相关操作，抛出IOException
    public void testOutStreamAfterFSClose() throws Exception {
        String fileName = "file";
        Path filePath = new Path(
            testRootPath + "/" + fileName);
        FSDataOutputStream outputStream = fs.create(filePath);
        outputStream.write(10);

        fs.close();

        boolean closeException = false;
        try {
            outputStream.close();
        } catch (IOException e) {
            closeException = true;
        }
        assertFalse(closeException);

        closeException = false;
        try {
            outputStream.flush();
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            outputStream.write(10);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            byte[] testBuffer = new byte[1024];

            for (int i = 0; i < testBuffer.length; ++i) {
                testBuffer[i] = (byte) (i % 255);
            }
            outputStream.write(testBuffer, 0 , 100);
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            outputStream.hsync();
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            outputStream.hflush();
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            outputStream.hsync();
        } catch (IOException e) {
            if (e.getMessage().equals("OBSFilesystem closed")) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // InputStream关闭后再执行InputStream相关操作，抛出IOException
    public void testInputStreamClose() throws Exception {
        final int bufLen = 256 * 1024;
        final int sizeMB = 10;
        String fileName = "readTestFile_" + sizeMB + ".txt";
        Path readTestFile = new Path(
            testRootPath + "/" + fileName);
        long size = sizeMB * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, readTestFile, size, 256,
            255);

        FSDataInputStream instream = this.fs.open(readTestFile);

        instream.close();

        boolean closeException = false;
        try {
            instream.getPos();
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            instream.seek(1024);
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            instream.seekToNewSource(1024);
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            instream.read();
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bufLen);
            instream.read(byteBuffer);
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            byte[] buf = new byte[1024];
            instream.read(buf, 0, 1024);
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            instream.available();
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            byte[] buf = new byte[1024];
            instream.readFully(1024, buf, 0, 1024);
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
                if (fs.getMetricSwitch()) {
                    MockMetricsConsumer mmc
                            = (MockMetricsConsumer) fs.getMetricsConsumer();
                    assertEquals(BasicMetricsConsumer.MetricKind.abnormal, mmc.getMr().getKind());
                    LOG.warn("metricInfo:"+mmc.getMr().getExceptionIns());
                }
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            byte[] buf = new byte[1024];
            instream.read(1024, buf, 0, 1024);
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
                if (fs.getMetricSwitch()) {
                    MockMetricsConsumer mmc
                            = (MockMetricsConsumer) fs.getMetricsConsumer();
                    assertEquals(BasicMetricsConsumer.MetricKind.abnormal, mmc.getMr().getKind());
                    LOG.warn("metricInfo:"+mmc.getMr().getExceptionIns());
                }
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            instream.setReadahead(1024L);
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }

    @Test
    // OutputStream关闭后再执行OutputStream相关操作，抛出IOException
    public void testOutStreamClose() throws Exception {
        String fileName = "file";
        Path filePath = new Path(
            testRootPath + "/" + fileName);
        FSDataOutputStream outputStream = fs.create(filePath);
        outputStream.write(10);

        outputStream.close();

        boolean closeException = false;
        try {
            outputStream.flush();
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            outputStream.write(10);
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            byte[] testBuffer = new byte[1024];

            for (int i = 0; i < testBuffer.length; ++i) {
                testBuffer[i] = (byte) (i % 255);
            }
            outputStream.write(testBuffer, 0 , 100);
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            outputStream.hsync();
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            outputStream.hflush();
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
            }
        }
        assertTrue(closeException);

        closeException = false;
        try {
            outputStream.hsync();
        } catch (IOException e) {
            if (e.getMessage().contains(FSExceptionMessages.STREAM_IS_CLOSED)) {
                closeException = true;
            }
        }
        assertTrue(closeException);
    }
}

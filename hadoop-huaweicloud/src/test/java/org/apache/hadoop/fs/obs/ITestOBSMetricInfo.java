/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.fs.obs;

import static org.apache.hadoop.fs.obs.OBSConstants.FAST_UPLOAD_BYTEBUFFER;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyObject;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.junit.*;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;

public class ITestOBSMetricInfo {
    private OBSFileSystem fs;

    private Configuration conf;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private static final String TEST_FILE = "dest_file";

    private OBSFileSystem mockFs;

    private ObsClient obsClient;

    private ObsClient mockObsClient;

    private Path testFile = getTestPath("testFile");

    private Path testDir = getTestPath("testDir");

    private static byte[] dataSet = ContractTestUtils.dataset(16, 0, 10);

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        conf = OBSContract.getConfiguration(null);
        conf.setClass(OBSConstants.OBS_METRICS_CONSUMER,
            MockMetricsConsumer.class, BasicMetricsConsumer.class);
        conf.setBoolean(OBSConstants.METRICS_SWITCH, true);
        conf.setBoolean(OBSConstants.OBS_CONTENT_SUMMARY_ENABLE, true);
        conf.setBoolean(OBSConstants.OBS_CLIENT_DFS_LIST_ENABLE, true);
        conf.set(OBSConstants.FAST_UPLOAD_BUFFER, FAST_UPLOAD_BYTEBUFFER);
        fs = OBSTestUtils.createTestFileSystem(conf);
        obsClient = fs.getObsClient();
        OBSFSTestUtil.deletePathRecursive(fs, getTestPath(TEST_FILE));

        initTestEnv();

        initMock();
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
            fs.close();
        }
    }

    private void initTestEnv() throws Exception {
        FSDataOutputStream outputStream = fs.create(testFile, true);
        outputStream.write(dataSet);
        outputStream.close();
        assertTrue(fs.mkdirs(testDir));
        if (!fs.exists(testDir)) {
            throw new IOException("testDir not exist!");
        }
    }

    private void initMock() {
        mockFs = Mockito.spy(fs);
        mockObsClient = Mockito.spy(obsClient);
        Whitebox.setInternalState(mockFs, obsClient, mockObsClient);
    }

    private Path getTestPath(String relativePath) {
        return new Path(testRootPath + "/" + relativePath);
    }

    @Test
    // open一个文件,获取fileStatus的len
    public void testOpen003() throws Exception {
        if (fs.getMetricSwitch()) {
            Path testFile = getTestPath(TEST_FILE);
            FSDataOutputStream outputStream = fs.create(testFile, true);
            OBSFSTestUtil.writeData(outputStream, 1024);
            outputStream.close();

            try {
                fs.open(testFile, 2048);
            } catch (IOException e) {
                e.printStackTrace();

            }
            //getFileStatus正常监控指标
            fs.getFileStatus(testFile);
            MockMetricsConsumer mmc
                = (MockMetricsConsumer) fs.getMetricsConsumer();
            assertEquals(BasicMetricsConsumer.MetricKind.normal, mmc.getMr().getKind());
            OBSFSTestUtil.deletePathRecursive(fs, getTestPath(TEST_FILE));
        }
    }

    @Test
    // 测试MetricGetFileStatus
    public void testGetFileStatus() throws IOException {
        OBSFileConflictException fileConflictException = new OBSFileConflictException(
                "mock FileConflictException");
        FileNotFoundException fileNotFoundException = new FileNotFoundException(
                "mock FileNotFoundException");
        AccessControlException accessControlException =
                new AccessControlException("mock AccessControlException");
        OBSIOException obsioException = new OBSIOException("mock IOException",
                new ObsException("mock ObsException"));

        Mockito .doThrow(fileConflictException)
                .doCallRealMethod()
                .when(mockFs)
                .innerGetFileStatus(anyObject());
        try {
            mockFs.getFileStatus(testFile);
        }
        catch (FileNotFoundException e) {
            MockMetricsConsumer mmc
                    = (MockMetricsConsumer) fs.getMetricsConsumer();
            assertEquals(BasicMetricsConsumer.MetricKind.abnormal, mmc.getMr().getKind());
        }
    }

    @Test
    // create  路径的父目录是文件，抛出ParentNotDirectoryException
    public void testCreateAbnormal001() throws Exception {
        if (fs.getMetricSwitch()) {
            if (!fs.isFsBucket()) {
                return;
            }
            Path testFile = getTestPath("a001/b001/test_file");
            Path parentPath = testFile.getParent();

            FSDataOutputStream outputStream = null;
            boolean hasException = false;
            try {
                outputStream = OBSFSTestUtil.createStream(fs, parentPath);
                outputStream.close();
                fs.create(testFile, new FsPermission((short) 00644),
                    false, fs.getConf().getInt("io.file.buffer.size", 4096),
                    fs.getDefaultReplication(testFile),
                    fs.getDefaultBlockSize(testFile), null);

            } catch (ParentNotDirectoryException e) {
                hasException = true;
            } finally {
                if (outputStream != null) {
                    outputStream.close();
                }
            }

            OBSFSTestUtil.deletePathRecursive(fs, testFile.getParent());
        }
    }

    @Test
    // create的文件已存在，抛出FileAlreadyExistsException
    public void testCreateAbnormal002() throws Exception {
        if (fs.getMetricSwitch()) {
            if (!fs.isFsBucket()) {
                return;
            }
            Path testFile = getTestPath("a001/b001/test_file");

            FSDataOutputStream outputStream = null;
            try {
                outputStream = OBSFSTestUtil.createStream(fs, testFile, false);
            } finally {
                if (outputStream != null) {
                    outputStream.close();
                }
            }

            boolean hasException = false;
            try {
                EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
                flags.add(CreateFlag.APPEND);
                outputStream = fs.create(testFile.getParent(),
                    new FsPermission((short) 00644),
                    flags, 4096, (short) 3,
                    128 * 1024 * 1024, null, null);
            } catch (FileAlreadyExistsException e) {
                hasException = true;
            } finally {
                if (outputStream != null) {
                    outputStream.close();
                }
            }
            assertTrue(hasException);

            OBSFSTestUtil.deletePathRecursive(fs, testFile);
        }
    }

    @Test
    // createNonRecursive 路径的父目录是文件，抛出ParentNotDirectoryException
    public void testCreateAbnormal003() throws Exception {
        if (fs.getMetricSwitch()) {
            if (!fs.isFsBucket()) {
                return;
            }
            Path testFile = getTestPath("a001/b001/test_file");
            Path parentPath = testFile.getParent();

            FSDataOutputStream outputStream = null;
            boolean hasException = false;
            try {
                outputStream = OBSFSTestUtil.createStream(fs, parentPath);
                outputStream.close();
                EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
                flags.add(CreateFlag.APPEND);
                outputStream = fs.create(testFile,
                    new FsPermission((short) 00644),
                    flags, 4096, (short) 3,
                    128 * 1024 * 1024, null, null);
            } catch (ParentNotDirectoryException e) {
                hasException = true;
            } finally {
                if (outputStream != null) {
                    outputStream.close();
                }
            }

            assertTrue(hasException);
            OBSFSTestUtil.deletePathRecursive(fs, testFile.getParent());
        }
    }

    @Test
    public void testDeleteNormal001() throws Exception {
        if (fs.getMetricSwitch()) {
            if (!fs.isFsBucket()) {
                return;
            }
            Path testPath = getTestPath("a001/b001/test_file");
            fs.delete(testPath.getParent(), true);
            FSDataOutputStream outputStream = null;
            try {
                outputStream = fs.create(testPath,
                    new FsPermission((short) 00644),
                    false, 4096,
                    (short) 3, 128 * 1024 * 1024, null);
                OBSFSTestUtil.writeData(outputStream, 1 * 1024 * 1024);
                outputStream.hsync();
                fs.delete(testPath, true);
                String position = String.valueOf(1 * 1024 * 1024);
                MockMetricsConsumer mmc2 =
                    (MockMetricsConsumer) fs.getMetricsConsumer();
                assertEquals(BasicMetricsConsumer.MetricKind.normal, mmc2.getMr().getKind());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (outputStream != null) {
                    outputStream.close();
                }
            }
        }

    }

    //FileNotFoundException
    @Test
    public void testDeleteNormal002() {
        if (fs.getMetricSwitch()) {
            try {
                fs.delete(new Path(testRootPath), true);
                fs.delete(new Path(testRootPath), true);
            } catch (IOException e) {

            }
            MockMetricsConsumer mmc
                    = (MockMetricsConsumer) fs.getMetricsConsumer();
            assertEquals(BasicMetricsConsumer.MetricKind.normal, mmc.getMr().getKind());
        }
    }
}

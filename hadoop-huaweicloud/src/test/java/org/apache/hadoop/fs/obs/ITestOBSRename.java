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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;

import com.obs.services.ObsClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.security.AccessControlException;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ITestOBSRename {
    private OBSFileSystem fs;

    private static String testRootPath = OBSTestUtils.generateUniqueTestPath();

    private static final Logger LOG = LoggerFactory.getLogger(
        ITestOBSRename.class);

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        conf.setClass(OBSConstants.OBS_METRICS_CONSUMER,
            MockMetricsConsumer.class, BasicMetricsConsumer.class);
        conf.set(OBSConstants.MULTIPART_SIZE, String.valueOf(5 * 1024 * 1024));
        conf.setBoolean(OBSConstants.METRICS_SWITCH, true);
        fs = OBSTestUtils.createTestFileSystem(conf);
        if (fs.exists(new Path(testRootPath))) {
            fs.delete(new Path(testRootPath), true);
        }
        fs.mkdirs(new Path(testRootPath));
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }

    @Test
    //文件到文件：源文件存在，但目标文件不存在，Rename返回True
    public void testFileToFile01() throws IOException {
        String srcFile = "srcFile";
        String destFile = "destFile";
        Path srcFilePath = new Path(testRootPath + "/" + srcFile);
        Path destFilePath = new Path(testRootPath + "/" + destFile);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }
        if (fs.exists(destFilePath)) {
            fs.delete(destFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        boolean res = fs.rename(srcFilePath, destFilePath);
        if (fs.getMetricSwitch()) {
            MockMetricsConsumer mmc
                = (MockMetricsConsumer) fs.getMetricsConsumer();
            assertEquals(BasicMetricsConsumer.MetricKind.normal, mmc.getMr().getKind());

        }
        assertTrue(res);
        assertFalse(fs.exists(srcFilePath));
        assertTrue(fs.exists(destFilePath));

        fs.delete(destFilePath);
    }

    @Test
    //文件到文件：源文件和目标文件均不存在，Rename返回False
    public void testFileToFile02() throws IOException {
        String srcFile = "srcFile";
        String destFile = "destFile";
        Path srcFilePath = new Path(testRootPath + "/" + srcFile);
        Path destFilePath = new Path(testRootPath + "/" + destFile);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }
        if (fs.exists(destFilePath)) {
            fs.delete(destFilePath);
        }

        assertFalse(fs.rename(srcFilePath, destFilePath));
        assertFalse(fs.exists(srcFilePath));
        assertFalse(fs.exists(destFilePath));
    }

    @Test
    //文件到文件：源文件不存在，但目标文件已存在，基于HDFS原生语义，源文件不存在时，Rename返回False
    public void testFileToFile03() throws IOException {
        String srcFile = "srcFile";
        String destFile = "destFile";
        Path srcFilePath = new Path(testRootPath + "/" + srcFile);
        Path destFilePath = new Path(testRootPath + "/" + destFile);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }
        if (fs.exists(destFilePath)) {
            fs.delete(destFilePath);
        }

        FSDataOutputStream stream = fs.create(destFilePath);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        assertFalse(fs.rename(srcFilePath, destFilePath));
        assertFalse(fs.exists(srcFilePath));
        assertTrue(fs.exists(destFilePath));

        fs.delete(destFilePath);
    }

    @Test
    //文件到文件：源文件存在，但目标文件的父目录不存在，Rename返回False
    public void testFileToFile04() throws IOException {
        String srcFile = "srcFile";
        String nonexistParent = "parent";
        String destFile = "destFile";
        Path srcFilePath = new Path(testRootPath + "/" + srcFile);
        Path destParentDirPath = new Path(testRootPath + "/" + nonexistParent);
        Path destFilePath = new Path(
            testRootPath + "/" + nonexistParent + "/" + destFile);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }
        if (fs.exists(destParentDirPath)) {
            fs.delete(destParentDirPath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        assertFalse(fs.rename(srcFilePath, destFilePath));
        assertTrue(fs.exists(srcFilePath));
        assertFalse(fs.exists(destParentDirPath));
        assertFalse(fs.exists(destFilePath));

        fs.delete(srcFilePath);
    }

    @Test
    //文件到文件：源文件存在，但目标文件的Parent是一个文件，Rename抛ParentNotDirectoryException
    public void testFileToFile05() throws IOException {
        String srcFile = "srcFile";
        String nonexistParent = "parent";
        String destFile = "destFile";
        Path srcFilePath = new Path(testRootPath + "/" + srcFile);
        Path destParentDirPath = new Path(testRootPath + "/" + nonexistParent);
        Path destFilePath = new Path(
            testRootPath + "/" + nonexistParent + "/" + destFile);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }
        if (fs.exists(destParentDirPath)) {
            fs.delete(destParentDirPath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        stream = fs.create(destParentDirPath);
        data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        boolean expectedException = false;
        try {
            fs.rename(srcFilePath, destFilePath);
        } catch (ParentNotDirectoryException e) {
            if (fs.getMetricSwitch()) {
                MockMetricsConsumer mmc
                        = (MockMetricsConsumer) fs.getMetricsConsumer();
                assertEquals(BasicMetricsConsumer.MetricKind.abnormal, mmc.getMr().getKind());
            }
            expectedException = true;
        }
        assertTrue(expectedException);
        assertTrue(fs.exists(srcFilePath));
        assertTrue(fs.exists(destParentDirPath));
        assertFalse(fs.exists(destFilePath));

        fs.delete(srcFilePath);
        fs.delete(destParentDirPath);
    }

    @Test
    //文件到文件：源文件存在，目标文件也存在，Rename返回False
    public void testFileToFile06() throws IOException {
        String srcFile = "srcFile";
        String destFile = "destFile";
        Path srcFilePath = new Path(testRootPath + "/" + srcFile);
        Path destFilePath = new Path(testRootPath + "/" + destFile);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }
        if (fs.exists(destFilePath)) {
            fs.delete(destFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        stream = fs.create(destFilePath);
        data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        assertFalse(fs.rename(srcFilePath, destFilePath));
        assertTrue(fs.exists(srcFilePath));
        assertTrue(fs.exists(destFilePath));

        fs.delete(srcFilePath);
        fs.delete(destFilePath);
    }

    @Test
    //文件到文件：源文件存在，目标文件和源文件相同，Rename返回True
    public void testFileToFile07() throws IOException {
        String srcFile = "srcFile";
        Path srcFilePath = new Path(testRootPath + "/" + srcFile);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        assertTrue(fs.rename(srcFilePath, srcFilePath));
        assertTrue(fs.exists(srcFilePath));

        fs.delete(srcFilePath);
    }

    @Test
    // 文件到文件：src的父目录及上级目录不是一个目录，抛AccessControlException
    public void testFileToFile08() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }
        String srcFile = "a001/srcFile";
        String dstFile = "b001/dstFile";
        Path srcFilePath = new Path(testRootPath + "/" + srcFile);
        Path dstFilePath = new Path(testRootPath + "/" + dstFile);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }
        if (fs.exists(dstFilePath)) {
            fs.delete(dstFilePath);
        }
        fs.mkdirs(dstFilePath.getParent());
        FSDataOutputStream stream = fs.create(srcFilePath.getParent());
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        boolean hasException = false;
        try {
            fs.rename(srcFilePath, dstFilePath);
        } catch (AccessControlException e) {
            if (fs.getMetricSwitch()) {
                MockMetricsConsumer mmc
                        = (MockMetricsConsumer) fs.getMetricsConsumer();
                assertEquals(BasicMetricsConsumer.MetricKind.abnormal, mmc.getMr().getKind());
                LOG.warn("metricInfo:"+mmc.getMr().getExceptionIns());
            }
            hasException = true;
        }
        assertTrue(hasException);

        fs.delete(dstFilePath);
    }

    @Test
    // 文件到文件：dst的父目录及上级目录不是一个目录，抛ParentNotDirectoryException
    public void testFileToFile09() throws IOException {
        String srcFile = "a001/srcFile";
        String dstFile = "b001/dstFile";
        Path srcFilePath = new Path(testRootPath + "/" + srcFile);
        Path dstFilePath = new Path(testRootPath + "/" + dstFile);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }
        if (fs.exists(dstFilePath)) {
            fs.delete(dstFilePath);
        }

        fs.mkdirs(srcFilePath.getParent());
        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();
        stream = fs.create(dstFilePath.getParent());
        stream.write(data);
        stream.close();

        boolean hasException = false;
        try {
            fs.rename(srcFilePath, dstFilePath);
        } catch (ParentNotDirectoryException e) {
            if (fs.getMetricSwitch()) {
                MockMetricsConsumer mmc
                        = (MockMetricsConsumer) fs.getMetricsConsumer();
                assertEquals(BasicMetricsConsumer.MetricKind.abnormal, mmc.getMr().getKind());
                LOG.warn("metricInfo:"+mmc.getMr().getExceptionIns());
            }
            hasException = true;
        }
        assertTrue(hasException);

        fs.delete(srcFilePath);
    }

    @Test
    //文件到文件：src的父目录及上级目录不存在，返回false
    public void testFileToFile10() throws IOException {
        String srcFile = "a001/srcFile";
        String dstFile = "b001/dstFile";
        Path srcFilePath = new Path(testRootPath + "/" + srcFile);
        Path dstFilePath = new Path(testRootPath + "/" + dstFile);
        fs.delete(srcFilePath.getParent(), true);
        fs.delete(dstFilePath.getParent(), true);
        fs.mkdirs(dstFilePath.getParent());
        assertFalse(fs.rename(srcFilePath, dstFilePath));
        fs.delete(srcFilePath);
    }

    // @Test
    // //文件到文件：对象桶rename过程中删除src失败，返回false
    // public void testFileToFile11() throws IOException {
    //     if (fs.isFsBucket()) {
    //         return;
    //     }
    //     String srcFile = "a001/srcFile";
    //     String dstFile = "b001/dstFile";
    //     Path srcFilePath = new Path(testRootPath + "/" + srcFile);
    //     Path dstFilePath = new Path(testRootPath + "/" + dstFile);
    //     fs.delete(srcFilePath.getParent(), true);
    //     fs.delete(dstFilePath.getParent(), true);
    //
    //     fs.mkdirs(srcFilePath.getParent());
    //     fs.mkdirs(dstFilePath.getParent());
    //     FSDataOutputStream stream = fs.create(srcFilePath);
    //     byte[] data = ContractTestUtils.dataset(16, 'a', 26);
    //     stream.write(data);
    //     stream.close();
    //
    //     OBSFileSystem mockFs = Mockito.spy(fs);
    //     ObsClient client = fs.getObsClient();
    //     ObsClient mockClient = Mockito.spy(client);
    //     Whitebox.setInternalState(mockFs, String.valueOf(client), mockClient);
    //     IOException ioException = new IOException("mock IOException");
    //     Mockito.doThrow(ioException)
    //         .when(mockClient)
    //         .deleteObject(anyString(), anyString());
    //
    //     assertFalse(mockFs.rename(srcFilePath, dstFilePath));
    //
    //     fs.delete(srcFilePath);
    // }

    @Test
    //目录到文件：源目录存在，目标文件也存在，Rename返回False
    public void testDirToFile01() throws IOException {
        String srcDir = "srcDir";
        String destFile = "destFile";
        Path srcDirPath = new Path(testRootPath + "/" + srcDir);
        Path destFilePath = new Path(testRootPath + "/" + destFile);
        if (fs.exists(srcDirPath)) {
            fs.delete(srcDirPath);
        }
        if (fs.exists(destFilePath)) {
            fs.delete(destFilePath);
        }

        fs.mkdirs(srcDirPath);

        FSDataOutputStream stream = fs.create(destFilePath);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        assertFalse(fs.rename(srcDirPath, destFilePath));
        assertTrue(fs.exists(srcDirPath));
        assertTrue(fs.exists(destFilePath));

        fs.delete(srcDirPath);
        fs.delete(destFilePath);
    }

    @Test
    //目录到文件：src是根目录，返回false
    public void testDirToFile02() throws IOException {
        if (!fs.isFsBucket()) {
            return;
        }
        Path srcFilePath = new Path("/");
        Path dstFilePath = new Path(testRootPath + "/test_file");

        assertFalse(fs.rename(srcFilePath, dstFilePath));
    }

    @Test
    //目录到目录：源目录存在，但目标目录不存在，Rename返回True
    public void testDirToDir01() throws IOException {
        String srcDir = "srcDir";
        String destDir = "destDir";
        Path srcDirPath = new Path(testRootPath + "/" + srcDir);
        Path destDirPath = new Path(testRootPath + "/" + destDir);
        if (fs.exists(srcDirPath)) {
            fs.delete(srcDirPath);
        }
        if (fs.exists(destDirPath)) {
            fs.delete(destDirPath);
        }

        fs.mkdirs(srcDirPath);

        assertTrue(fs.rename(srcDirPath, destDirPath));
        assertFalse(fs.exists(srcDirPath));
        assertTrue(fs.exists(destDirPath));

        fs.delete(destDirPath);
    }

    @Test
    //目录到目录：源目录和目标目录均不存在，Rename返回False
    public void testDirToDir02() throws IOException {
        String srcDir = "srcDir";
        String destDir = "destDir";
        Path srcDirPath = new Path(testRootPath + "/" + srcDir);
        Path destDirPath = new Path(testRootPath + "/" + destDir);
        if (fs.exists(srcDirPath)) {
            fs.delete(srcDirPath);
        }
        if (fs.exists(destDirPath)) {
            fs.delete(destDirPath);
        }

        assertFalse(fs.rename(srcDirPath, destDirPath));
        assertFalse(fs.exists(srcDirPath));
        assertFalse(fs.exists(destDirPath));
    }

    @Test
    //目录到目录：源目录不存在，但目标目录已存在，基于HDFS原生语义，源目录不存在时，Rename返回False
    public void testDirToDir03() throws IOException {
        String srcDir = "srcDir";
        String destDir = "destDir";
        Path srcDirPath = new Path(testRootPath + "/" + srcDir);
        Path destDirPath = new Path(testRootPath + "/" + destDir);
        if (fs.exists(srcDirPath)) {
            fs.delete(srcDirPath);
        }
        if (fs.exists(destDirPath)) {
            fs.delete(destDirPath);
        }

        fs.mkdirs(destDirPath);

        assertFalse(fs.rename(srcDirPath, destDirPath));
        assertFalse(fs.exists(srcDirPath));
        assertTrue(fs.exists(destDirPath));

        fs.delete(destDirPath);
    }

    @Test
    //目录到目录：源目录存在，但目标目录的父目录不存在，Rename返回False
    public void testDirToDir04() throws IOException {
        String srcDir = "srcDir";
        String nonexistParent = "parent";
        String destDir = "destDir";
        Path srcDirPath = new Path(testRootPath + "/" + srcDir);
        Path destParentDirPath = new Path(testRootPath + "/" + nonexistParent);
        Path destDirPath = new Path(
            testRootPath + "/" + nonexistParent + "/" + destDir);
        if (fs.exists(srcDirPath)) {
            fs.delete(srcDirPath);
        }
        if (fs.exists(destParentDirPath)) {
            fs.delete(destParentDirPath);
        }

        fs.mkdirs(srcDirPath);

        assertFalse(fs.rename(srcDirPath, destDirPath));
        assertTrue(fs.exists(srcDirPath));
        assertFalse(fs.exists(destParentDirPath));
        assertFalse(fs.exists(destDirPath));

        fs.delete(srcDirPath);
    }

    @Test
    //目录到目录：目标目录是源目录下面的一个Child，Rename返回False
    public void testDirToDir05() throws IOException {
        String srcDir = "srcDir";
        String destDir = "destDir";
        Path srcDirPath = new Path(testRootPath + "/" + srcDir);
        Path destDirPath = new Path(testRootPath + "/" + destDir);
        if (fs.exists(srcDirPath)) {
            fs.delete(srcDirPath);
        }
        if (fs.exists(destDirPath)) {
            fs.delete(destDirPath);
        }

        Path childDestDirPath = new Path(
            testRootPath + "/" + srcDir + "/" + destDir);
        fs.mkdirs(srcDirPath);
        fs.mkdirs(childDestDirPath);

        assertFalse(fs.rename(srcDirPath, childDestDirPath));
        assertTrue(fs.exists(srcDirPath));

        fs.delete(srcDirPath);
    }

    @Test
    //目录到目录：源目录存在，目标目录存在，源目录被移到目标目录下作为Child，Rename返回True
    public void testDirToDir06() throws IOException {
        String srcDir = "srcDir";
        String srcFile = "srcFile";
        String destDir = "destDir";
        Path srcDirPath = new Path(testRootPath + "/" + srcDir);
        Path srcFilePath = new Path(
            testRootPath + "/" + srcDir + "/" + srcFile);
        Path destDirPath = new Path(testRootPath + "/" + destDir);
        if (fs.exists(srcDirPath)) {
            fs.delete(srcDirPath);
        }
        if (fs.exists(destDirPath)) {
            fs.delete(destDirPath);
        }

        fs.mkdirs(srcDirPath);
        fs.mkdirs(destDirPath);

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        assertTrue(fs.rename(srcDirPath, destDirPath));
        assertFalse(fs.exists(srcDirPath));
        assertTrue(fs.exists(
            new Path(testRootPath + "/" + destDir + "/" + srcDir + "/")));
        assertTrue(fs.exists(new Path(
            testRootPath + "/" + destDir + "/" + srcDir + "/" + srcFile)));

        fs.delete(destDirPath);
    }

    @Test
    //目录到目录：源目录存在，目标目录存在，源目录被移到目标目录下作为Child，但目标目录下该目录已存在，Rename返回False
    public void testDirToDir07() throws IOException {
        String srcDir = "srcDir";
        String srcFile = "srcFile";
        String destDir = "destDir";
        Path srcDirPath = new Path(testRootPath + "/" + srcDir);
        Path srcFilePath = new Path(
            testRootPath + "/" + srcDir + "/" + srcFile);
        Path destDirPath = new Path(testRootPath + "/" + destDir);
        Path destChildDirPath = new Path(
            testRootPath + "/" + destDir + "/" + srcDir);
        Path destFilePath = new Path(
            testRootPath + "/" + destDir + "/" + srcDir + "/" + srcFile);
        if (fs.exists(srcDirPath)) {
            fs.delete(srcDirPath);
        }
        if (fs.exists(destDirPath)) {
            fs.delete(destDirPath);
        }

        fs.mkdirs(srcDirPath);
        fs.mkdirs(destDirPath);
        fs.mkdirs(destChildDirPath);

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        assertFalse(fs.rename(srcDirPath, destDirPath));
        assertTrue(fs.exists(srcDirPath));
        assertTrue(fs.exists(destChildDirPath));
        assertFalse(fs.exists(destFilePath));

        fs.delete(srcDirPath);
        fs.delete(destDirPath);
    }

    @Test
    //目录到目录：源目录存在，目标目录和源目录相同，Rename返回False
    public void testDirToDir08() throws IOException {
        String srcDir = "srcDir";
        Path srcDirPath = new Path(testRootPath + "/" + srcDir);
        if (fs.exists(srcDirPath)) {
            fs.delete(srcDirPath);
        }

        fs.mkdirs(srcDirPath);

        assertFalse(fs.rename(srcDirPath, srcDirPath));
        assertTrue(fs.exists(srcDirPath));

        fs.delete(srcDirPath);
    }

    @Test
    //目录到目录：src的父目录及上级目录不存在，Rename返回False
    public void testDirToDir09() throws IOException {
        String srcDir = "a001/a002/srcDir";
        String dstDir = "b001/b002/dstDir";
        Path srcDirPath = new Path(testRootPath + "/" + srcDir);
        Path dstDirPath = new Path(testRootPath + "/" + dstDir);
        fs.delete(srcDirPath.getParent(), true);
        fs.delete(dstDirPath, true);

        fs.mkdirs(dstDirPath.getParent());
        assertFalse(fs.rename(srcDirPath, dstDirPath));
        fs.delete(srcDirPath);
    }

    @Test
    //目录到目录：src是根目录，返回false
    public void testDirToDir10() throws IOException {
        if (!fs.isFsBucket()) {
            return;
        }
        String srcDir = "/";
        String dstDir = "dstDir";
        Path srcDirPath = new Path(srcDir);
        Path dstDirPath = new Path(testRootPath + "/" + dstDir);
        fs.delete(dstDirPath, true);
        fs.mkdirs(dstDirPath);

        assertFalse(fs.rename(srcDirPath, dstDirPath));
        fs.delete(dstDirPath);
    }

    @Test
    //目录到目录：对象桶rename过程中删除src失败，返回false
    public void testDirToDir11() throws IOException {
        if (!fs.isFsBucket()) {
            return;
        }
        if (fs.isFsBucket()) {
            return;
        }
        String srcDir = "a001/srcDir";
        String dstDir = "b001/dstDir";
        Path srcDirPath = new Path(testRootPath + "/" + srcDir);
        Path dstDirPath = new Path(testRootPath + "/" + dstDir);
        fs.delete(srcDirPath.getParent(), true);
        fs.delete(dstDirPath.getParent(), true);

        fs.mkdirs(srcDirPath);
        fs.mkdirs(dstDirPath.getParent());

        OBSFileSystem mockFs = Mockito.spy(fs);
        ObsClient client = fs.getObsClient();
        ObsClient mockClient = Mockito.spy(client);
        Whitebox.setInternalState(mockFs, String.valueOf(client), mockClient);
        IOException ioException = new IOException("mock IOException");
        Mockito.doThrow(ioException)
            .when(mockClient)
            .deleteObject(anyString(), anyString());

        assertFalse(mockFs.rename(srcDirPath, dstDirPath));

        fs.delete(srcDirPath);
        fs.delete(dstDirPath.getParent());
    }

    @Test
    //目录到目录：源目录存在，但目标目录的父目录为文件或不存在，Rename返回True
    public void testDirToDir12() throws IOException {
        if (!fs.isFsBucket()) {
            return;
        }
        String srcDir = "a/b/srcDir";
        String destDir = "e/d/destDir";
        Path srcDirPath = new Path(testRootPath + "/" + srcDir);
        Path destDirPath = new Path(testRootPath + "/" + destDir);
        if (fs.exists(srcDirPath)) {
            fs.delete(srcDirPath);
        }
        if (fs.exists(destDirPath)) {
            fs.delete(destDirPath);
        }

        fs.mkdirs(srcDirPath);

        FSDataOutputStream stream = fs.create(
            destDirPath.getParent().getParent());
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();
        boolean hasException = false;
        try {
            fs.rename(srcDirPath, destDirPath);
        } catch (ParentNotDirectoryException e) {
            if (fs.getMetricSwitch()) {
                MockMetricsConsumer mmc
                        = (MockMetricsConsumer) fs.getMetricsConsumer();
                assertEquals(BasicMetricsConsumer.MetricKind.abnormal, mmc.getMr().getKind());
                LOG.warn("metricInfo:"+mmc.getMr().getExceptionIns());
            }
            hasException = true;
        }
        assertTrue(hasException);
    }

    @Test
    //目录到目录：源目录中存在超过1000个文件，rename返回True
    public void testDirToDir13() throws IOException {
        if (fs.isFsBucket()) {
            return;
        }

        String srcDir = "srcDir";
        String destDir = "destDir";
        Path srcDirPath = new Path(testRootPath + "/" + srcDir);
        Path destDirPath = new Path(testRootPath + "/" + destDir);

        if (fs.exists(srcDirPath)) {
            fs.delete(srcDirPath,true);
        }
        fs.mkdirs(srcDirPath);
        for (int i=0; i<1001;i++){
            Path srcFilePath = new Path(
                    testRootPath + "/" + srcDir + "/" + srcDir + i);
            FSDataOutputStream stream = fs.create(srcFilePath);
            stream.close();
        }

        if (fs.exists(destDirPath)) {
            fs.delete(destDirPath,true);
        }
        fs.mkdirs(destDirPath);
//        boolean rename = fs.rename(srcDirPath, destDirPath);
        assertTrue(fs.rename(srcDirPath, destDirPath));

    }

    @Test
    //文件到目录：源文件存在，目标目录存在，源文件被移到目标目录下作为Child，Rename返回True
    public void testFileToDir01() throws IOException {
        String srcFile = "srcFile";
        String destDir = "destDir";
        Path srcFilePath = new Path(testRootPath + "/" + srcFile);
        Path destDirPath = new Path(testRootPath + "/" + destDir);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }
        if (fs.exists(destDirPath)) {
            fs.delete(destDirPath);
        }

        fs.mkdirs(destDirPath);

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        assertTrue(fs.rename(srcFilePath, destDirPath));
        assertFalse(fs.exists(srcFilePath));
        assertTrue(
            fs.exists(new Path(testRootPath + "/" + destDir + "/" + srcFile)));

        fs.delete(destDirPath);
    }

    @Test
    //文件到目录：源文件存在，目标目录存在，源文件被移到目标目录下作为Child，但目标目录下该文件已存在，Rename返回False
    public void testFileToDir02() throws IOException {
        String srcFile = "srcFile";
        String destDir = "destDir";
        Path srcFilePath = new Path(testRootPath + "/" + srcFile);
        Path destDirPath = new Path(testRootPath + "/" + destDir);
        Path destFilePath = new Path(
            testRootPath + "/" + destDir + "/" + srcFile);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }
        if (fs.exists(destDirPath)) {
            fs.delete(destDirPath);
        }

        fs.mkdirs(destDirPath);

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        stream = fs.create(destFilePath);
        data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();

        assertFalse(fs.rename(srcFilePath, destDirPath));
        assertTrue(fs.exists(srcFilePath));
        assertTrue(fs.exists(destDirPath));
        assertTrue(fs.exists(destFilePath));

        fs.delete(srcFilePath);
        fs.delete(destDirPath);
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

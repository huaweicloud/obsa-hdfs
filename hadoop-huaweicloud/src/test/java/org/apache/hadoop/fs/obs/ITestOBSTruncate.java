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
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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

import java.io.FileNotFoundException;
import java.io.IOException;

public class ITestOBSTruncate {
    private OBSFileSystem fs;

    private static String testRootPath = OBSTestUtils.generateUniqueTestPath();

    private final int partSize = 5 * 1024 * 1024;

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        conf.set(OBSConstants.MULTIPART_SIZE, String.valueOf(partSize));
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
    // create一个文件并写入数据5MB-1Byte（长度小于段大小），
    // 之后再truncate到10Byte，预期截断成功
    public void truncate01() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize - 1, 'a',
            26);
        stream.write(data);
        stream.close();

        long newLength = 10;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据5MB（长度等于段大小），
    // 之后再truncate到10Byte，预期截断成功
    public void truncate02() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize, 'a',
            26);
        stream.write(data);
        stream.close();

        long newLength = 10;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据5MB+100Byte（长度大于段大小），
    // 之后再truncate到段大小5MB，预期截断成功
    public void truncate03() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize + 100, 'a',
            26);
        stream.write(data);
        stream.close();

        long newLength = partSize;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据5MB+100Byte（长度大于段大小），
    // 之后再truncate到超过段大小5MB+10Byte，预期截断成功
    public void truncate04() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize + 100, 'a',
            26);
        stream.write(data);
        stream.close();

        long newLength = partSize + 10;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据100Byte（长度小于段大小），
    // 然后append一些数据500Byte（长度仍然小于段大小），
    // 之后再truncate到10Byte，预期截断成功
    public void truncate05() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(500, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = 10;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据100Byte（长度小于段大小），
    // 然后append一些数据500Byte（长度仍然小于段大小），
    // 之后再truncate到200Byte，预期截断成功
    public void truncate06() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(500, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = 200;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据100Byte（长度小于段大小），
    // 然后append一些数据5MB（长度超过段大小），
    // 之后再truncate到200Byte，预期截断成功
    public void truncate07() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(partSize, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = 200;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据100Byte（长度小于段大小），
    // 然后append一些数据5MB（长度超过段大小），
    // 之后再truncate到5MB，预期截断成功
    public void truncate08() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(partSize, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = partSize;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据100Byte（长度小于段大小），
    // 然后append一些数据5MB（长度超过段大小），
    // 之后再truncate到5MB+10Byte，预期截断成功
    public void truncate09() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(partSize, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = partSize + 10;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据5MB+100Byte（长度大于段大小），
    // 然后append一些数据500Byte，
    // 之后再truncate到段大小5MB，预期截断成功
    public void truncate10() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize + 100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(500, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = partSize;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据5MB+100Byte（长度大于段大小），
    // 然后append一些数据500Byte，
    // 之后再truncate到超过段大小5MB+10Byte，预期截断成功
    public void truncate11() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize + 100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(500, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = partSize + 10;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据5MB+100Byte（长度大于段大小），
    // 然后append一些数据500Byte，
    // 之后再truncate到超过段大小5MB+200Byte，预期截断成功
    public void truncate12() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize + 100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(500, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = partSize + 200;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据5MB+100Byte（长度大于段大小），
    // 然后append一些数据5MB，
    // 之后再truncate到超过段大小5MB+200Byte，预期截断成功
    public void truncate13() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize + 100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(partSize, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = partSize + 200;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据5MB+100Byte（长度大于段大小），
    // 然后append一些数据5MB，
    // 之后再truncate到两个段大小10MB，预期截断成功
    public void truncate14() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize + 100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(partSize, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = partSize * 2;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据100Byte（长度大于段大小），
    // 然后append一些数据20MB，
    // 之后再truncate到两个段大小10MB+10Byte，预期截断成功
    public void truncate15() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(partSize * 4, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = partSize * 2 + 10;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据5MB+100Byte（长度大于段大小），
    // 然后append一些数据20MB，
    // 之后再truncate到两个段大小15MB+10Byte，预期截断成功
    public void truncate16() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize + 100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(partSize * 4, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = partSize * 3 + 10;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据5MB+100Byte（长度大于段大小），
    // 然后append一些数据20MB，
    // 之后再truncate到两个段大小15MB+10Byte，预期截断成功
    public void truncate17() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize + 100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(partSize * 4, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = partSize * 3 + 10;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        newLength = partSize * 2 + 10;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        newLength = partSize + 10;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        newLength = partSize;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        newLength = partSize - 10;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // 在目标路径的父目录是一个文件的情况下，
    // 执行truncate，预期抛AccessControlException
    public void truncate18() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String parentDir = "parentDir";
        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + parentDir +  "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath.getParent());
        byte[] data = ContractTestUtils.dataset(100, 'a',
            26);
        stream.write(data);
        stream.close();

        boolean hasException = false;
        try {
            fs.truncate(srcFilePath, 10);
        } catch (AccessControlException e) {
            hasException = true;
        }
        assertTrue(hasException);

        fs.delete(srcFilePath.getParent());
    }

    @Test
    // 在目标路径是一个目录的情况下，
    // 执行truncate，预期抛FileNotFoundException
    public void truncate19() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String parentDir = "parentDir";
        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + parentDir +  "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        fs.mkdirs(srcFilePath);

        boolean hasException = false;
        try {
            fs.truncate(srcFilePath, 1);
        } catch (FileNotFoundException e) {
            hasException = true;
        }
        assertTrue(hasException);

        fs.delete(srcFilePath.getParent());
    }

    @Test
    // create一个文件并写入数据5MB-1Byte（长度小于段大小），
    // 之后再truncate到5MB-1Byte（长度不变），预期截断成功
    public void truncate20() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize - 1, 'a',
            26);
        stream.write(data);
        stream.close();

        long newLength = partSize - 1;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据5MB+100Byte（长度大于段大小），
    // 之后再truncate到5MB+100Byte（长度不变），预期截断成功
    public void truncate21() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize + 100, 'a',
            26);
        stream.write(data);
        stream.close();

        long newLength = partSize + 100;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据100Byte（长度小于段大小），
    // 然后append一些数据5MB（长度超过段大小），
    // 之后再truncate到5MB+100Byte（长度不变），预期截断成功
    public void truncate22() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(partSize, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = partSize + 100;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据5MB-1Byte（长度小于段大小），
    // 之后再truncate到0，预期截断成功
    public void truncate23() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize - 1, 'a',
            26);
        stream.write(data);
        stream.close();

        long newLength = 0;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据5MB+100Byte（长度大于段大小），
    // 之后再truncate到0，预期截断成功
    public void truncate24() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(partSize + 100, 'a',
            26);
        stream.write(data);
        stream.close();

        long newLength = 0;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据100Byte（长度小于段大小），
    // 然后append一些数据5MB（长度超过段大小），
    // 之后再truncate到0，预期截断成功
    public void truncate25() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(partSize, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = 0;
        assertTrue(fs.truncate(srcFilePath, newLength));
        assertEquals(newLength, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据100Byte（长度小于段大小），
    // 然后append一些数据5MB（长度超过段大小），
    // 之后再truncate到5MB+200Byte（长度变大），
    // 预期抛HadoopIllegalArgumentException
    public void truncate26() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(partSize, 'a', 26);
        stream.write(data);
        stream.close();

        long newLength = partSize + 200;
        boolean hasException = false;
        try {
            fs.truncate(srcFilePath, newLength);
        } catch (IOException e) {
            if(e.getCause() instanceof HadoopIllegalArgumentException){
                hasException = true;
            }
        }
        assertTrue(hasException);
        assertEquals(partSize + 100, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据100Byte（长度小于段大小），但未close文件，
    // 之后再truncate到10Byte，预期抛AlreadyBeingCreatedException
    public void truncate27() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(100, 'a',
            26);
        stream.write(data);
        stream.hflush();

        long newLength = 10;
        boolean hasException = false;
        try {
            fs.truncate(srcFilePath, newLength);
        } catch (OBSAlreadyBeingCreatedException e) {
            hasException = true;
        }
        assertTrue(hasException);
        stream.close();
        assertEquals(100, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }

    @Test
    // create一个文件并写入数据100Byte（长度小于段大小），
    // 然后append一些数据5MB（长度超过段大小），但未close文件，
    // 之后再truncate到5MB，预期抛AlreadyBeingCreatedException
    public void truncate28() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }

        String fileName = "file";
        Path srcFilePath = new Path(
            testRootPath + "/" + fileName);
        if (fs.exists(srcFilePath)) {
            fs.delete(srcFilePath);
        }

        FSDataOutputStream stream = fs.create(srcFilePath);
        byte[] data = ContractTestUtils.dataset(100, 'a',
            26);
        stream.write(data);
        stream.close();

        stream = fs.append(srcFilePath);
        data = ContractTestUtils.dataset(partSize, 'a', 26);
        stream.write(data);

        long newLength = partSize;
        boolean hasException = false;
        try {
            fs.truncate(srcFilePath, newLength);
        } catch (OBSAlreadyBeingCreatedException e) {
            hasException = true;
        }
        assertTrue(hasException);
        stream.close();
        assertEquals(partSize + 100, fs.getFileStatus(srcFilePath).getLen());

        fs.delete(srcFilePath);
    }
}

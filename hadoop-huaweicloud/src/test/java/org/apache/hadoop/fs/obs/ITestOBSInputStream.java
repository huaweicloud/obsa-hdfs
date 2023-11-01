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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.fs.obs.input.InputPolicyFactory;
import org.apache.hadoop.fs.obs.input.InputPolicys;
import org.apache.hadoop.fs.obs.input.ReadAheadBuffer;
import org.apache.hadoop.fs.obs.input.ReadAheadTask;
import org.apache.hadoop.fs.obs.mock.MockMemArtsCCClient;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

/**
 * Tests basic functionality for OBSInputStream, including seeking and
 * reading files.
 */
@RunWith(Parameterized.class)
public class ITestOBSInputStream {

    private OBSFileSystem fs;

    private static final Logger LOG =
        LoggerFactory.getLogger(ITestOBSInputStream.class);

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private String inputPolicy;

    @Parameterized.Parameters
    public static Collection inputStreams() {
        return Arrays.asList(
            OBSConstants.READAHEAD_POLICY_PRIMARY,
            // OBSConstants.READAHEAD_POLICY_ADVANCE,
            OBSConstants.READAHEAD_POLICY_MEMARTSCC
            );
    }

    public ITestOBSInputStream(String inputPolicy) throws IOException, NoSuchFieldException, IllegalAccessException {
        this.inputPolicy = inputPolicy;
        if (inputPolicy.equals(OBSConstants.READAHEAD_POLICY_PRIMARY)) {
            Configuration conf = OBSContract.getConfiguration(null);
            conf.setClass(OBSConstants.OBS_METRICS_CONSUMER,
                MockMetricsConsumer.class, BasicMetricsConsumer.class);
            conf.setBoolean(OBSConstants.METRICS_SWITCH, true);
            conf.set(OBSConstants.READAHEAD_POLICY, OBSConstants.READAHEAD_POLICY_PRIMARY);
            fs = OBSTestUtils.createTestFileSystem(conf);
        }
        if (inputPolicy.equals(OBSConstants.READAHEAD_POLICY_MEMARTSCC)) {
            Configuration conf = OBSContract.getConfiguration(null);
            conf.setClass(OBSConstants.OBS_METRICS_CONSUMER, MockMetricsConsumer.class, BasicMetricsConsumer.class);
            conf.setBoolean(OBSConstants.METRICS_SWITCH, true);
            conf.set(OBSConstants.READAHEAD_POLICY, OBSConstants.READAHEAD_POLICY_MEMARTSCC);
            conf.set(OBSConstants.MEMARTSCC_INPUTSTREAM_BUFFER_TYPE, OBSConstants.MEMARTSCC_INPUTSTREAM_BUFFER_TYPE_BIND);
            this.fs = OBSTestUtils.createTestFileSystem(conf);

            MockMemArtsCCClient mockMemArtsCCClient;
            mockMemArtsCCClient = new MockMemArtsCCClient(fs, false, this.fs.getBucket());
            mockMemArtsCCClient.init("", "");

            InputPolicyFactory inputPolicyFactory = InputPolicys.createFactory(OBSConstants.READAHEAD_POLICY_MEMARTSCC);

            // mock memartscc client
            Field ccClient = OBSFileSystem.class.getDeclaredField("memArtsCCClient");
            ccClient.setAccessible(true);
            ccClient.set(this.fs, mockMemArtsCCClient);

            // mock input policy factory
            Field fInputPolicyFactory = OBSFileSystem.class.getDeclaredField("inputPolicyFactory");
            fInputPolicyFactory.setAccessible(true);
            fInputPolicyFactory.set(this.fs, inputPolicyFactory);
        }
    }

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @Rule
    public Timeout testTimeout = new Timeout(30 * 60 * 1000);

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
        }
    }

    private Path setPath(String path) {
        if (path.startsWith("/")) {
            return new Path(testRootPath + path);
        } else {
            return new Path(testRootPath + "/" + path);
        }
    }

    private static void genTestBuffer(byte[] buffer, long lenth) {
        for (int i = 0; i < lenth; i++) {
            buffer[i] = (byte) (i % 255);
        }
    }

    @Test
    // 校验inputstream默认readahead大小为1M
    public void testDefaultReadAheadSize() {
        long readAHeadSize = fs.getReadAheadRange();
        assertTrue("Default read ahead size must 1MB",
            readAHeadSize == 1 * 1024 * 1024);
    }

    @Test
    public void testFileReaderTask() throws Exception {
        Path smallSeekFile = setPath("/test/testFileReaderTask.txt");
        long size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, smallSeekFile, size, 256, 255);
        LOG.info("5MB file created: smallSeekFileOSSFileReader.txt");
        ReadAheadBuffer readBuffer = new ReadAheadBuffer(12, 24);
        ReadAheadTask task = new ReadAheadTask(fs.getBucket(),"1",
            fs.getObsClient(), readBuffer);
        //NullPointerException, fail
        task.run();
        assertEquals(readBuffer.getStatus(), ReadAheadBuffer.STATUS.ERROR);
        //OK
        task = new ReadAheadTask(fs.getBucket(),"test/test/testFileReaderTask.txt",
            fs.getObsClient(), readBuffer);
        task.run();
        assertEquals(readBuffer.getStatus(), ReadAheadBuffer.STATUS.SUCCESS);
    }

    @Test
    // 打开文件流，按固定长度和随机长度分别向前seek 5次
    public void testInSeekFile() throws Exception {
        Path smallSeekFile = setPath("/test/smallSeekFile.txt");
        long size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, smallSeekFile, size, 256,
            255);
        LOG.info("5MB file created: smallSeekFile.txt");

        FSDataInputStream instream = this.fs.open(smallSeekFile);
        int seekTimes = 5;
        LOG.info("multiple fold position seeking test...:");
        for (int i = 0; i < seekTimes; i++) {
            long pos = size / (seekTimes - i) - 1;
            LOG.info("begin seeking for pos: " + pos);
            instream.seek(pos);
            assertTrue("expected position at:" + pos + ", but got:"
                + instream.getPos(), instream.getPos() == pos);
            LOG.info("completed seeking at pos: " + instream.getPos());
        }
        LOG.info("random position seeking test...:");
        Random rand = new Random();
        for (int i = 0; i < seekTimes; i++) {
            long pos = Math.abs(rand.nextLong()) % size;
            LOG.info("begin seeking for pos: " + pos);
            instream.seek(pos);
            assertTrue("expected position at:" + pos + ", but got:"
                + instream.getPos(), instream.getPos() == pos);
            LOG.info("completed seeking at pos: " + instream.getPos());
        }
        IOUtils.closeStream(instream);
    }

    @Test
    // 打开文件输入流，分别测试随机和顺序读取功能正确性
    public void testSequentialAndRandomRead() throws Exception {
        Path smallSeekFile = setPath("/test/smallSeekFile.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, smallSeekFile, size, 256,
            255);
        LOG.info("5MB file created: smallSeekFile.txt");

        long now = System.currentTimeMillis();
        FSDataInputStream fsDataInputStream = this.fs.open(smallSeekFile);
        // OBSInputStream in =
        //     (OBSInputStream) fsDataInputStream.getWrappedStream();
        assertTrue("expected position at:" + 0 + ", but got:"
            + fsDataInputStream.getPos(), fsDataInputStream.getPos() == 0);

        byte[] bytes = new byte[(int) size];
        int bytesRead = 0;
        int bufOffset = 0;
        int bytesRemaining = size;
        int ret = 0;
        while (bytesRead < size) {
            ret = fsDataInputStream.read(bytes, bufOffset, bytesRemaining);
            //        System.out.println(ret);

            if (ret < 0) {
                break;
            }

            bufOffset += ret;
            bytesRemaining -= ret;
            bytesRead += ret;
        }

        LOG.warn("time consumed mills：" + String.valueOf(
            System.currentTimeMillis() - now));

        byte[] testBuffer = new byte[256];
        genTestBuffer(testBuffer, 256);

        byte[] equalSizeBuffer = new byte[(int) size];

        for (int i = 0; i < equalSizeBuffer.length; i++) {
            equalSizeBuffer[i] = testBuffer[(i) % 256];
        }
        assertTrue(Arrays.equals(bytes, equalSizeBuffer));

        fsDataInputStream.close();
        //fsDataInputStream.seek(4 * 1024 * 1024);

    }

    @Test
    // 打开文件输入流，测试read byteBuffer功能正确性
    public void testReadWithByteBuffer() throws Exception {
        final int bufLen = 256 * 1024;
        final int sizeMB = 100;
        String filename = "readTestFile_" + sizeMB + ".txt";
        Path readTestFile = setPath("/test/" + filename);
        long size = sizeMB * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, readTestFile, size, 256,
            255);
        LOG.info(sizeMB + "MB file created: /test/" + filename);

        FSDataInputStream instream = this.fs.open(readTestFile);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bufLen);
        long expectedRound = size / bufLen + (size % bufLen == 0 ? 0 : 1);
        long bytesRead = 0;
        long round = 0;
        while (bytesRead < size) {
            byteBuffer.clear();
            round++;
            int bytes = instream.read(byteBuffer);
            if (bytes == -1) {
                break;
            }

            if (round < expectedRound) {
                assertEquals(bufLen, bytes);
            } else {
                assertEquals(size - bytesRead, bytes);
            }

            bytesRead += bytes;

            if (bytesRead % (1024 * 1024) == 0) {
                int available = instream.available();
                int remaining = (int) (size - bytesRead);
                assertTrue("expected remaining:" + remaining + ", but got:"
                        + available,
                    remaining == available);
                LOG.info("Bytes read: " + Math.round(
                    (double) bytesRead / (1024 * 1024))
                    + " MB");
            }

            byteBuffer.flip();
            for (int i = 0; i < bytes; i++) {
                byteBuffer.get();
            }
            assertEquals(0, byteBuffer.remaining());
        }
        assertEquals(0, instream.available());
        assertEquals(expectedRound, round);
        assertEquals(size, bytesRead);
        IOUtils.closeStream(instream);
    }

    @Test
    // 打开文件流，测试read(byte[] buf, final int off, final int len)接口正确性
    public void testReadFile() throws Exception {
        final int bufLen = 256;
        final int sizeFlag = 5;
        String filename = "readTestFile_" + sizeFlag + ".txt";
        Path readTestFile = setPath("/test/" + filename);
        long size = sizeFlag * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, readTestFile, size, 256,
            255);
        LOG.info(sizeFlag + "MB file created: /test/" + filename);

        FSDataInputStream instream = this.fs.open(readTestFile);
        byte[] buf = new byte[bufLen];
        long bytesRead = 0;
        while (bytesRead < size) {
            int bytes;
            if (size - bytesRead < bufLen) {
                int remaining = (int) (size - bytesRead);
                bytes = instream.read(buf, 0, remaining);
            } else {
                bytes = instream.read(buf, 0, bufLen);
            }
            bytesRead += bytes;

            if (bytesRead % (1024 * 1024) == 0) {
                int available = instream.available();
                int remaining = (int) (size - bytesRead);
                assertTrue("expected remaining:" + remaining + ", but got:"
                        + available,
                    remaining == available);
                LOG.info("Bytes read: " + Math.round(
                    (double) bytesRead / (1024 * 1024))
                    + " MB");
            }
        }
        assertTrue(instream.available() == 0);
        IOUtils.closeStream(instream);
    }

    @Test
    // 打开文件输入流，测试read(long position, byte[] buffer, int offset, int length)接口正确性
    public void testReadByPosistion() throws IOException {
        Path smallSeekFile = setPath("/test/smallSeekFile.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, smallSeekFile, size, 256,
            255);
        LOG.info("5MB file created: smallSeekFile.txt");

        long now = System.currentTimeMillis();
        FSDataInputStream fsDataInputStream = this.fs.open(smallSeekFile);
        // OBSInputStream in
        //     = (OBSInputStream) fsDataInputStream.getWrappedStream();
        assertTrue("expected position at:" + 0 + ", but got:"
                + fsDataInputStream.getPos(),
            fsDataInputStream.getPos() == 0);

        byte[] testBuffer = new byte[256];
        genTestBuffer(testBuffer, 256);

        int length = 1024 * 1024;
        int pos = 0;
        byte[] randomBytes = new byte[length];
        int len = fsDataInputStream.read(pos, randomBytes, 0, length);
        // if (fs.getMetricSwitch()) {
        //     MockMetricsConsumer mmc
        //         = (MockMetricsConsumer) fs.getMetricsConsumer();
        //     assertEquals("read", mmc.getOpName());
        //     assertTrue(mmc.isSuccess());
        //     assertEquals("random", mmc.getOpType());
        //
        //     assertTrue(length == len);
        //     LOG.warn("random read len: " + len);
        // }
        byte[] equalsRandomBuffer = new byte[length];
        for (int i = 0; i < equalsRandomBuffer.length; i++) {
            equalsRandomBuffer[i] = testBuffer[(i + pos) % 256];
        }
        assertTrue(Arrays.equals(randomBytes, equalsRandomBuffer));

        int overlapSize = 1024;
        len = fsDataInputStream.read(size - overlapSize, randomBytes, 0, length);
        assertEquals(overlapSize, len);

        len = fsDataInputStream.read(size, randomBytes, 0, length);
        assertEquals(-1, len);

        LOG.warn("time consumed mills：" + String.valueOf(
            System.currentTimeMillis() - now));

        fsDataInputStream.close();
    }

    @Test
    // 读取位置超过文件大小时，校验是否返回-1
    public void testReadPositionExceedFileLength() throws IOException {
        Path smallSeekFile = setPath("/test/smallSeekFile.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, smallSeekFile, size, 256,
            255);
        LOG.info("5MB file created: smallSeekFile.txt");

        long now = System.currentTimeMillis();
        FSDataInputStream fsDataInputStream = this.fs.open(smallSeekFile);
        // OBSInputStream in
        //     = (OBSInputStream) fsDataInputStream.getWrappedStream();
        assertTrue("expected position at:" + 0 + ", but got:"
                + fsDataInputStream.getPos(),
            fsDataInputStream.getPos() == 0);

        byte[] testBuffer = new byte[256];
        genTestBuffer(testBuffer, 256);

        int length = 10 * 1024 * 1024;
        int pos = size;
        byte[] randomBytes = new byte[length];
        int len = fsDataInputStream.read(pos, randomBytes, 0, length);
        assertTrue(len == -1);

        pos = size + 1;
        len = fsDataInputStream.read(pos, randomBytes, 0, length);
        assertTrue(len == -1);
        fsDataInputStream.close();

    }

    @Test
    // 读取长度超过文件大小时，校验是否成功读到文件末尾
    public void testReadLengthExceedFileLength() throws IOException {
        if (inputPolicy.equals(OBSConstants.READAHEAD_POLICY_MEMARTSCC)) {
            return;
            /**
             * 争议用例：hadoop PositionedReadable接口中的
             * int read(long position, byte[] buffer, int offset, int length)方法
             * 没有规定：返回值必需要等于min(buffer.size, length)，即必需要读满buffer
             * 而该用例假设：读取5M数据，返回读取成功大小一定等于5M
              */
        }
        Path smallSeekFile = setPath("/test/smallSeekFile.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, smallSeekFile, size, 256,
            255);
        LOG.info("5MB file created: smallSeekFile.txt");

        long now = System.currentTimeMillis();
        FSDataInputStream fsDataInputStream = this.fs.open(smallSeekFile);
        // OBSInputStream in
        //     = (OBSInputStream) fsDataInputStream.getWrappedStream();
        assertTrue("expected position at:" + 0 + ", but got:"
                + fsDataInputStream.getPos(),
            fsDataInputStream.getPos() == 0);

        byte[] testBuffer = new byte[256];
        genTestBuffer(testBuffer, 256);

        int length = 10 * 1024 * 1024;
        int pos = 0;
        byte[] randomBytes = new byte[length];
        int len = fsDataInputStream.read(pos, randomBytes, 0, length);
        assertTrue(size == len);
        fsDataInputStream.close();
    }

    @Test
    // 读取长度超过文件大小时，校验是否成功读到文件末尾
    public void testReadLengthExceedFileLength1() throws IOException {
        Path smallSeekFile = setPath("/test/smallSeekFile1.txt");
        int size = 5 * 1024;

        ContractTestUtils.generateTestFile(this.fs, smallSeekFile, size, 256,
            255);
        LOG.info("1MB file created: smallSeekFile1.txt");

        long now = System.currentTimeMillis();
        FSDataInputStream fsDataInputStream = this.fs.open(smallSeekFile);
        // OBSInputStream in
        //     = (OBSInputStream) fsDataInputStream.getWrappedStream();
        assertTrue("expected position at:" + 0 + ", but got:"
                + fsDataInputStream.getPos(),
            fsDataInputStream.getPos() == 0);

        byte[] testBuffer = new byte[256];
        genTestBuffer(testBuffer, 256);

        int length = 1 * 1024 * 1024;
        int pos = 0;
        byte[] randomBytes = new byte[length];
        int len = fsDataInputStream.read(pos, randomBytes, 0, length);
        assertTrue(size == len);
        fsDataInputStream.close();
    }

    @Test
    // 校验四个参数read转三个参数read开关是否能正确配置生效
    public void testReadTransformSwitch() throws Exception {
        final int sizeFlag = 5;
        String filename = "readTestFile_" + sizeFlag + ".txt";
        Path readTestFile = setPath("/test/" + filename);
        long size = sizeFlag * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, readTestFile, size, 256,
            255);
        LOG.info(sizeFlag + "MB file created: /test/" + filename);

        byte[] buffer = new byte[32];
        FSDataInputStream fsDataInputStream = this.fs.open(readTestFile);
        FSInputStream obsInputStream
            = (FSInputStream) fsDataInputStream.getWrappedStream();
        FSInputStream mockInputStream = Mockito.spy(obsInputStream);
        System.out.println(mockInputStream);
        Mockito.doReturn(-100)
            .when((FSInputStream) mockInputStream)
            .read(buffer, 0, 10);
        int readLen = mockInputStream.read(0, buffer, 0, 10);
        assertTrue(readLen == -100);
        IOUtils.closeStream(fsDataInputStream);
        fs.close();
        fs = null;

        Configuration conf = OBSContract.getConfiguration(null);
        conf.setBoolean(OBSConstants.READAHEAD_TRANSFORM_ENABLE, false);
        OBSFileSystem obsFs = OBSTestUtils.createTestFileSystem(conf);
        fsDataInputStream = obsFs.open(readTestFile);
        obsInputStream = (FSInputStream) fsDataInputStream.getWrappedStream();
        mockInputStream = Mockito.spy(obsInputStream);
        Mockito.when(mockInputStream.read(buffer, 0, 10))
            .then((Answer<Integer>) invocationOnMock -> -100);
        readLen = mockInputStream.read(0, buffer, 0, 10);
        assertTrue(readLen == 10);
        IOUtils.closeStream(fsDataInputStream);
        obsFs.close();
    }

    @Test
    public void testSeekOutOfRange1() throws IOException {
        Path smallSeekFile = setPath("/test/seekOutOfRangeFile1.txt");
        long size = 100;

        ContractTestUtils.generateTestFile(this.fs, smallSeekFile, size, 256,
            255);
        LOG.info("5MB file created: smallSeekFile.txt");

        FSDataInputStream instream = this.fs.open(smallSeekFile);
        instream.seek(size);
    }

    @Test(expected = EOFException.class)
    public void testSeekOutOfRange2() throws IOException {
        Path smallSeekFile = setPath("/test/seekOutOfRangeFile2.txt");
        long size = 100;

        ContractTestUtils.generateTestFile(this.fs, smallSeekFile, size, 256,
            255);
        LOG.info("5MB file created: smallSeekFile.txt");

        FSDataInputStream instream = this.fs.open(smallSeekFile);
        instream.seek(size + 1);
    }


    // @Test
    // public void testRead() throws Exception {
    //     if (fs.getMetricSwitch()) {
    //         if (!fs.isFsBucket()) {
    //             return;
    //         }
    //         Path testFile = setPath("test_file");
    //         if (fs.exists(testFile)) {
    //             fs.delete(testFile);
    //         }
    //         final int fileSize = 1024;
    //
    //         FSDataOutputStream outputStream = fs.create(testFile);
    //         byte[] data5 = ContractTestUtils.dataset(fileSize, 'a',
    //             26);
    //         outputStream.write(data5);
    //         outputStream.hsync();
    //         FSDataInputStream inputStream;
    //         inputStream = fs.open(testFile);
    //         int res = inputStream.read();
    //
    //         MockMetricsConsumer mmc
    //             = (MockMetricsConsumer) fs.getMetricsConsumer();
    //         assertEquals("read", mmc.getOpName());
    //         assertEquals("1byte", mmc.getOpType());
    //         assertTrue(mmc.isSuccess());
    //         inputStream.close();
    //
    //         inputStream = fs.open(testFile);
    //         ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);
    //         int res1 = inputStream.read(byteBuffer);
    //         MockMetricsConsumer mmc1 =
    //             (MockMetricsConsumer) fs.getMetricsConsumer();
    //         assertEquals("read", mmc1.getOpName());
    //         assertEquals("byteBuf", mmc1.getOpType());
    //         assertTrue(mmc1.isSuccess());
    //         inputStream.close();
    //
    //         inputStream = fs.open(testFile);
    //         byte[] bytes = new byte[fileSize];
    //         int res2 = inputStream.read(bytes, 0, 1);
    //         MockMetricsConsumer mmc2 =
    //             (MockMetricsConsumer) fs.getMetricsConsumer();
    //         assertEquals("read", mmc2.getOpName());
    //         assertEquals("seq", mmc2.getOpType());
    //         assertTrue(mmc2.isSuccess());
    //         inputStream.close();
    //
    //         inputStream = fs.open(testFile);
    //         byte[] bytes2 = new byte[fileSize];
    //         long position = 2;
    //         int offset = 1;
    //         int length = 4;
    //         inputStream.readFully(position, bytes2, offset, length);
    //         MockMetricsConsumer mmc3 =
    //             (MockMetricsConsumer) fs.getMetricsConsumer();
    //         assertEquals("readFully", mmc3.getOpName());
    //         assertTrue(mmc3.isSuccess());
    //         inputStream.close();
    //
    //         inputStream = fs.open(testFile);
    //         byte[] bytes3 = new byte[fileSize];
    //         long position1 = 3;
    //         int offset1 = 4;
    //         int length1 = 5;
    //         int readSize = inputStream.read(position1, bytes3, offset1,
    //             length1);
    //         MockMetricsConsumer mmc4 =
    //             (MockMetricsConsumer) fs.getMetricsConsumer();
    //         assertEquals("read", mmc4.getOpName());
    //         assertEquals("random", mmc4.getOpType());
    //         assertTrue(mmc4.isSuccess());
    //         inputStream.close();
    //
    //         fs.delete(testFile, false);
    //     }
    // }
}

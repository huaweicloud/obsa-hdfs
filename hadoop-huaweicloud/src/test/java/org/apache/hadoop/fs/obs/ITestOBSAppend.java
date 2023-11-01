package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.Random;
import java.util.UUID;

import static org.apache.hadoop.fs.contract.ContractTestUtils.NanoTimer;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.fs.contract.ContractTestUtils.bandwidth;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyReceivedData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ITestOBSAppend {
    private OBSFileSystem fs;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private int testBufferSize;

    private int modulus;

    private byte[] testBuffer;

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
        conf.setBoolean(OBSConstants.METRICS_SWITCH, true);
        conf.set(OBSConstants.MULTIPART_SIZE, String.valueOf(5 * 1024 * 1024));
        fs = OBSTestUtils.createTestFileSystem(conf);
        testBufferSize = fs.getConf().getInt(ContractTestUtils.IO_CHUNK_BUFFER_SIZE, 128);
        modulus = fs.getConf().getInt(ContractTestUtils.IO_CHUNK_MODULUS_SIZE, 128);
        testBuffer = new byte[testBufferSize];

        for (int i = 0; i < testBuffer.length; ++i) {
            testBuffer[i] = (byte) (i % modulus);
        }
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

    /**
     * 文件桶，对一个已经存在的文件做追加（每次小于multipart size），追加3次
     *
     * @throws Exception
     */
    @Test
    public void testAppendPosixNormal001() throws Exception {
        verifyAppend(1024 * 1024 * 10, 1024, 3, 4096);
    }

    /**
     * 文件桶，对一个已经存在的文件做追加（每次小于multipart size），追加10次
     *
     * @throws Exception
     */
    @Test
    public void testAppendPosixNormal002() throws Exception {
        verifyAppend(1024 * 1024 * 10, 1024, 10, 4096);
    }

    /**
     * 文件桶，对一个已经存在的文件做追加(每次大于multipart size)，追加3次
     *
     * @throws Exception
     */
    @Test
    public void testAppendPosixNormal003() throws Exception {
        verifyAppend(1024 * 1024 * 10, 1024 * 1024 * 10, 3, 4096);
    }

    /**
     * 文件桶，对一个已经存在的文件做追加(每次大于multipart size)，追加10次
     *
     * @throws Exception
     */
    @Test
    public void testAppendPosixNormal004() throws Exception {
        verifyAppend(1024 * 1024 * 10, 1024 * 1024 * 10, 10, 4096);
    }

    /**
     * 文件桶，追加多次，小于和大于100M交替
     *
     * @throws Exception
     */
    @Test
    public void testAppendPosixNormal005() throws Exception {
        verifyCrossSizeAppend(1024 * 1024 * 10, 1024, 1024 * 1024 * 200,
            1024 * 1024 * 100, 3, 4096);
    }

    /**
     * 文件桶，追加多次，每次追加随机大小
     *
     * @throws Exception
     */
    @Test
    public void testAppendPosixNormal006() throws Exception {
        verifyMultiSizeAppend(1024 * 1024 * 10, 1024, 1024 * 1024 * 200, 3,
            4096);
    }

    /**
     * 文件桶，追加多次，每次追加1字节
     *
     * @throws Exception
     */
    @Test
    public void testAppendPosixNormal007() throws Exception {
        verifyAppend(1024 * 1024 * 10, 1, 30, 4096);
    }

    /**
     * 文件桶，追加写大于100M，中间中断后继续追加
     * @throws Exception
     */
    //    @Test
    //    public void testAppendPosixNormal008() throws Exception {
    //
    //    }

    /**
     * 对象桶创建append stream，抛UnsupportedOperationException
     *
     * @throws Exception
     */
    @Test
    public void testAppendPosixAbnormal001() throws Exception {
        if (fs.isFsBucket()) {
            return;
        }
        Path testFile = getTestPath("a001/test_file");
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
        boolean hasException = false;
        FSDataOutputStream outputStream = fs.create(testFile, false);
        outputStream.close();
        try {
            outputStream = fs.append(testFile, 4096, null);
        } catch (UnsupportedOperationException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertTrue(
            "create append stream in object bucket should throw UnsupportedOperationException",
            hasException);

        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    /**
     * 路径为一个不存在的文件，抛出FileNotFoundException
     *
     * @throws Exception
     */
    @Test
    public void testAppendPosixAbnormal002() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFile = getTestPath("a001/test_file");
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
        boolean hasException = false;
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.append(testFile, 4096, null);
        } catch (FileNotFoundException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertTrue(
            "append on non exist file should throw FileNotFoundException",
            hasException);
    }

    /**
     * 路径为一个目录，抛出FileAlreadyExistsException
     *
     * @throws Exception
     */
    @Test
    public void testAppendPosixAbnormal003() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testPath = getTestPath("test-dir");
        fs.mkdirs(testPath);
        boolean hasException = false;
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.append(testPath, 4096, null);
        } catch (FileAlreadyExistsException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertTrue(
            "append on an exist directory should throw FileAlreadyExistsException",
            hasException);
        OBSFSTestUtil.deletePathRecursive(fs, testPath);
    }

    /**
     * 文件的父目录及上级目录不存在，抛FileNotFoundException
     *
     * @throws Exception
     */
    @Test
    public void testAppendPosixAbnormal004() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testPath = getTestPath("a001/b001/test_file");
        fs.delete(testPath.getParent(), true);
        boolean hasException = false;
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.append(testPath, 4096, null);
        } catch (FileNotFoundException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertTrue(
            "append on file whose parent not exists should throw FileNotFoundException",
            hasException);
    }

    /**
     * 文件的父目录及上级目录非目录，抛AccessControlException
     *
     * @throws Exception
     */
    @Test
    public void testAppendPosixAbnormal005() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testPath = getTestPath("a001/b001/test_file");
        fs.delete(testPath.getParent(), true);
        FSDataOutputStream outputStream = fs.create(testPath.getParent(),
            false);
        outputStream.close();

        boolean hasException = false;
        try {
            outputStream = fs.append(testPath, 4096, null);
        } catch (AccessControlException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertTrue(
            "append on file whose parent is not directory should throw AccessControlException",
            hasException);
    }

    private void verifyAppend(long fileSize, long appendSize, int appendTimes,
        int bufferSize)
        throws IOException {
        if (!fs.isFsBucket()) {
            return;
        }
        long total = fileSize;
        Path objectPath = createAppendFile(fileSize);
        for (int i = 0; i < appendTimes; i++) {
            appendFileByBuffer(objectPath, appendSize, bufferSize, total);
            total = total + appendSize;
        }
        verifyReceivedData(fs, objectPath, total, testBufferSize, modulus);
        OBSFSTestUtil.deletePathRecursive(fs, objectPath);
    }

    private void verifyCrossSizeAppend(long fileSize, int appendMinSize,
        int appendMaxSize,
        int mediumSize, int appendTimes, int bufferSize)
        throws IOException {
        if (!fs.isFsBucket()) {
            return;
        }
        long totoal = fileSize;
        Random random = new Random();
        Path objectPath = createAppendFile(fileSize);
        for (int i = 0; i < appendTimes; i++) {
            int appendSize =
                random.nextInt(appendMaxSize) % (appendMaxSize - appendMinSize
                    + 1) + appendMinSize;
            if (i % 2 == 0) {
                appendSize = appendSize > mediumSize
                    ? appendSize - mediumSize
                    : appendSize;
            } else {
                appendSize = appendSize < mediumSize
                    ? appendSize + mediumSize
                    : appendSize;
            }
            appendFileByBuffer(objectPath, appendSize, bufferSize, totoal);
            totoal = totoal + appendSize;
        }
        verifyReceivedData(fs, objectPath, totoal, testBufferSize, modulus);
    }

    private void verifyMultiSizeAppend(long fileSize, int appendMinSize,
        int appendMaxSize,
        int appendTimes, int bufferSize)
        throws IOException {
        if (!fs.isFsBucket()) {
            return;
        }
        long totoal = fileSize;
        Random random = new Random();
        Path objectPath = createAppendFile(fileSize);
        for (int i = 0; i < appendTimes; i++) {
            int appendSize =
                random.nextInt(appendMaxSize) % (appendMaxSize - appendMinSize
                    + 1) + appendMinSize;
            appendFileByBuffer(objectPath, appendSize, bufferSize, totoal);
            totoal = totoal + appendSize;
        }
        verifyReceivedData(fs, objectPath, totoal, testBufferSize, modulus);
    }

    private Path createAppendFile(long fileSize) throws IOException {
        String objectName = UUID.randomUUID().toString();
        Path objectPath = getTestPath(objectName);
        NanoTimer timer = new NanoTimer();

        OutputStream outputStream = createAppendStream(objectPath);
        writStream(outputStream, fileSize, 0);
        bandwidth(timer, fileSize);
        assertPathExists(fs, "not created successful", objectPath);
        return objectPath;
    }

    private void appendFileByBuffer(Path objectPath, long appendSize,
        int bufferSize, long offset) throws IOException {
        OutputStream outputStream = fs.append(objectPath, bufferSize, null);
        writStream(outputStream, appendSize, offset);
        assertPathExists(fs, "not created successful", objectPath);
    }

    private void writStream(OutputStream outputStream, long fileSize,
        long offset) throws IOException {
        long bytesWritten = 0L;
        Throwable var10 = null;
        long diff;
        try {
            int off = (int) (offset % testBuffer.length);
            while (bytesWritten < fileSize) {
                diff = fileSize - bytesWritten;
                if (diff + off <= (long) testBuffer.length) {
                    outputStream.write(testBuffer, off, (int) diff);
                    bytesWritten += diff;
                    break;
                } else {
                    outputStream.write(testBuffer, off,
                        (testBuffer.length - off));
                    bytesWritten += (long) testBuffer.length - off;
                }
                off = 0;
            }
        } catch (Throwable var21) {
            var10 = var21;
            throw var21;
        } finally {
            if (outputStream != null) {
                if (var10 != null) {
                    try {
                        outputStream.close();
                    } catch (Throwable var20) {
                        var10.addSuppressed(var20);
                    }
                } else {
                    outputStream.close();
                }
            }

        }
        assertEquals(fileSize, bytesWritten);
    }

    private FSDataOutputStream createAppendStream(Path objectPath)
        throws IOException {
        EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
        flags.add(CreateFlag.APPEND);
        FsPermission permission = new FsPermission((short) 00644);
        return fs.create(objectPath, permission, flags,
            fs.getConf().getInt("io.file.buffer.size", 4096),
            fs.getDefaultReplication(objectPath),
            fs.getDefaultBlockSize(objectPath),
            null, null);
    }

    @Test
    public void testAppendNormal() throws IOException {
        if (fs.getMetricSwitch()) {
            long fileSize = 10;
            long appendSize = 10;
            int bufferSize = 4096;
            if (!fs.isFsBucket()) {
                return;
            }
            long total = fileSize + appendSize;

            Path objectPath = createAppendFile(fileSize);
            // appendFileByBuffer(objectPath, appendSize, bufferSize, total);
            OutputStream outputStream = fs.append(objectPath, bufferSize, null);
            writStream(outputStream, appendSize, total);

            OBSFSTestUtil.deletePathRecursive(fs, objectPath);
        }
    }
}

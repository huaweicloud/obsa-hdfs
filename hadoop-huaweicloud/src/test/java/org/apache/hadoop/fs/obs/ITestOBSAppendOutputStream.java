package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.UUID;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.junit.Assert.assertEquals;

public class ITestOBSAppendOutputStream {

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
        conf.setBoolean(OBSConstants.FAST_UPLOAD, true);
        conf.set(OBSConstants.MULTIPART_SIZE, String.valueOf(5 * 1024 * 1024));
        fs = OBSTestUtils.createTestFileSystem(conf);
        testBufferSize = fs.getConf().getInt(
            ContractTestUtils.IO_CHUNK_BUFFER_SIZE, 128);
        modulus = fs.getConf().getInt(ContractTestUtils.IO_CHUNK_MODULUS_SIZE
            + ".size", 128);
        testBuffer = new byte[testBufferSize];

        for (int i = 0; i < testBuffer.length; ++i) {
            testBuffer[i] = (byte) (i % modulus);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }

    private Path getTestPath() {
        return new Path(testRootPath + "/test-obs");
    }

    private Path getRenameTestPath() {
        return new Path(testRootPath + "/test-obs-rename");
    }

    @Test
    // append空文件，校验append后大小
    public void testZeroSizeFileAppend() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        verifyAppend(0, 1024, 3, 4096);
    }

    @Test
    // 单次write小于缓存块大小，多次append，写满本地缓存块，触发上传到服务端，校验大小
    public void testBellowMultipartSizeAppend() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        // 在128字节 buffer写（单次write小于设置的5MB缓存的append），即会走OBSBlockOutputStream的275行逻辑
        verifyAppend(1024 * 1024, 1024 * 1024 * 80, 3, 4096);
        verifyAppend(1024 * 1024 * 10, 1024 * 1024 * 4, 3, 4096);
        verifyAppend(1024 * 1024, 1024, 10, 4096);
        verifyAppend(1024 * 1024, 1024 * 1024, 3, 4096);
    }

    @Test
    // 单次write大于缓存块大小，多次append，写满本地缓存块，触发上传到服务端，校验大小
    public void testAboveMultipartSizeAppend() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        // 在6MB buffer写（单次write大于设置的5MB缓存的append），即会走OBSBlockOutputStream的265行逻辑
        testBufferSize = fs.getConf()
            .getInt(ContractTestUtils.IO_CHUNK_BUFFER_SIZE, 6 * 1024 * 1024);
        modulus = fs.getConf().getInt(ContractTestUtils.IO_CHUNK_MODULUS_SIZE, 128);
        testBuffer = new byte[testBufferSize];

        for (int i = 0; i < testBuffer.length; ++i) {
            testBuffer[i] = (byte) (i % modulus);
        }
        verifyAppend(1024 * 1024, 1024 * 1024 * 11, 3, 6 * 1024 * 1024);
        verifyAppend(1024 * 1024 * 10, 1024 * 1024 * 10, 3, 6 * 1024 * 1024);
        verifyAppend(1024 * 1024, 1024, 10, 6 * 1024 * 1024);
        verifyAppend(1024 * 1024, 1024 * 1024, 3, 6 * 1024 * 1024);
    }

    @Test
    // 文件rename后，append目标文件，校验大小正常
    public void testAppendAfterRename() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        long fileSize = 1024 * 1024;
        long total = fileSize;
        int appendTimes = 3;
        long appendSize = 1024;
        Path objectPath = createAppendFile(fileSize);
        for (int i = 0; i < appendTimes; i++) {
            appendFile(objectPath, appendSize, total);
            total = total + appendSize;
        }

        verifyReceivedData(fs, objectPath, total, testBufferSize, modulus);

        String objectName = objectPath.getName();
        Path renamePath = new Path(getRenameTestPath(), objectName);
        fs.mkdirs(getRenameTestPath());
        fs.rename(objectPath, renamePath);

        for (int i = 0; i < appendTimes; i++) {
            appendFile(renamePath, appendSize, total);
            total = total + appendSize;
        }

        verifyReceivedData(fs, renamePath, total, testBufferSize, modulus);
    }

    @Test
    // append流被关闭后，再写数据，抛出IOException
    public void testAppendAfterClose() throws IOException {
        if (!fs.isFsBucket()) {
            return;
        }

        Path dest = createAppendFile(1024);
        OutputStream stream = creatAppendStream(dest);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        try {
            stream.write(data);
            stream.close();
            boolean hasException = false;
            try {
                stream.write(data);
            } catch (IOException e) {
                hasException = true;
            }
            assertTrue(hasException);

        } finally {
            fs.delete(dest, false);
            if (stream != null) {
                stream.close();
            }
        }
    }

    private void verifyAppend(long fileSize, long appendSize, int appendTimes,
        int bufferSize) throws IOException {
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
    }

    private Path createAppendFile(long fileSize) throws IOException {

        String objectName = UUID.randomUUID().toString();
        Path objectPath = new Path(getTestPath(), objectName);
        ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();

        OutputStream outputStream = creatAppendStream(objectPath);
        writStream(outputStream, fileSize, 0);
        bandwidth(timer, fileSize);
        assertPathExists(fs, "not created successful", objectPath);
        return objectPath;
    }

    private void appendFile(Path objectPath, long appendSize, long offset)
        throws IOException {
        OutputStream outputStream = fs.append(objectPath, 4096, null);
        writStream(outputStream, appendSize, offset);
        assertPathExists(fs, "not created successful", objectPath);
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

    private OutputStream creatAppendStream(Path objectPath) throws IOException {
        EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
        flags.add(CreateFlag.APPEND);
        FsPermission permission = new FsPermission((short) 00644);
        return fs.create(objectPath, permission, flags,
            fs.getConf().getInt("io.file.buffer.size", 4096),
            fs.getDefaultReplication(objectPath),
            fs.getDefaultBlockSize(objectPath), null);
    }
}


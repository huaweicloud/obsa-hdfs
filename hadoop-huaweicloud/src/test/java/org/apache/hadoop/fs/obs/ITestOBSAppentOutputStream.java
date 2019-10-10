package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.UUID;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.junit.Assert.assertEquals;

public class ITestOBSAppentOutputStream {

    private OBSFileSystem fs;
    private static String testRootPath =
            OBSTestUtils.generateUniqueTestPath();
    private int testBufferSize;
    private int modulus;
    private byte[] testBuffer;
    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean(Constants.FAST_UPLOAD, true);
        conf.set("fs.obs.multipart.size", String.valueOf(5 * 1024 * 1024));
        fs = OBSTestUtils.createTestFileSystem(conf);
        testBufferSize = fs.getConf().getInt("io.chunk.buffer.size", 128);
        modulus = fs.getConf().getInt("io.chunk.modulus.size", 128);
        testBuffer = new byte[testBufferSize];

        for(int i = 0; i < testBuffer.length; ++i) {
            testBuffer[i] = (byte)(i % modulus);
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
    public void testZeroAppend()throws Exception{
        verifyAppend(0, 1024, 3, 4096);
    }

    @Test
    public void testMulitAppend()throws Exception{
        // 在128字节 buffer写（单次write小于设置的5MB缓存的append），即会走OBSBlockOutputStream的275行逻辑
        verifyAppend(1024*1024, 1024*1024*80, 3, 4096);
        verifyAppend(1024*1024*1024, 1024*1024*4, 3, 4096);
        verifyAppend(1024*1024, 1024, 1000, 4096);
        verifyAppend(1024*1024, 1024*1024, 3000, 4096);
    }

    @Test
    public void testNotEqualMulitAppend()throws Exception{
        // 在6MB buffer写（单次write大于设置的5MB缓存的append），即会走OBSBlockOutputStream的265行逻辑
        testBufferSize = fs.getConf().getInt("io.chunk.buffer.size", 6*1024*1024);
        modulus = fs.getConf().getInt("io.chunk.modulus.size", 128);
        testBuffer = new byte[testBufferSize];

        for(int i = 0; i < testBuffer.length; ++i) {
            testBuffer[i] = (byte)(i % modulus);
        }
        verifyAppend(1024*1024, 1024*1024*11, 3, 6 * 1024 * 1024);
        verifyAppend(1024*1024*1024, 1024*1024*10, 3, 6 * 1024 * 1024);
        verifyAppend(1024*1024, 1024, 1000, 6 * 1024 * 1024);
        verifyAppend(1024*1024, 1024*1024, 3000, 6 * 1024 * 1024);
    }
    @Test
    public void testAppendAfterRename()throws Exception
    {
        long fileSize = 1024*1024;
        long total = 1024*1024;
        int appendTimes = 3;
        long appendSize = 1024;
        Path objectPath = createAppendFile(fileSize);
        for(int i = 0;i < appendTimes; i++) {
            appendFile(objectPath, appendSize);
            total =total + appendSize;
        }

        verifyReceivedData(fs, objectPath, total, testBufferSize, modulus);

        String objectName = objectPath.getName();
        Path renamePath = new Path(getRenameTestPath(), objectName);
        fs.mkdirs(getRenameTestPath());
        fs.rename(objectPath, renamePath);

        for(int i = 0;i < appendTimes; i++) {
            appendFile(renamePath, appendSize);
            total =total + appendSize;
        }

        verifyReceivedData(fs, renamePath, total, testBufferSize, modulus);
    }

    @Test(expected = IOException.class)
    public void testAppendAfterClose()throws Throwable{

        Path dest = createAppendFile(1024);
        OutputStream stream = creatAppendStream(dest);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        try {
            stream.write(data);
            stream.close();
            stream.write(data);

        } finally {
            fs.delete(dest, false);
            IOUtils.closeStream(stream);
        }
    }



    private void verifyAppend(long fileSize, long appendSize, int appendTimes, int bufferSize) throws IOException{
        long total = fileSize;
        Path objectPath = createAppendFile( fileSize);
        for(int i = 0;i < appendTimes; i++) {
            appendFileByBuffer(objectPath, appendSize, bufferSize);
            total =total + appendSize;
        }
        verifyReceivedData(fs, objectPath, total, testBufferSize, modulus);
    }

    private Path createAppendFile(long fileSize)throws IOException{

        String objectName = UUID.randomUUID().toString();
        Path objectPath = new Path(getTestPath(), objectName);
        ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();

        OutputStream outputStream = creatAppendStream(objectPath);
        writStream(outputStream, fileSize);
        bandwidth(timer, fileSize);
        assertPathExists(fs, "not created successful", objectPath);
        return objectPath;
    }
    private void appendFile(Path objectPath, long appendSize)throws IOException{
        OutputStream outputStream = fs.append(objectPath,4096,null);
        writStream(outputStream,appendSize);
        assertPathExists(fs, "not created successful", objectPath);
    }

    private void appendFileByBuffer(Path objectPath, long appendSize, int bufferSize)throws IOException{
        OutputStream outputStream = fs.append(objectPath,bufferSize,null);
        writStream(outputStream,appendSize);
        assertPathExists(fs, "not created successful", objectPath);
    }

    private void writStream( OutputStream outputStream, long fileSize)throws IOException{
        long bytesWritten = 0L;
        Throwable var10 = null;
        long diff;
        try {
            while(bytesWritten < fileSize) {
                diff = fileSize - bytesWritten;
                if (diff < (long)testBuffer.length) {
                    outputStream.write(testBuffer, 0, (int)diff);
                    bytesWritten += diff;
                } else {
                    outputStream.write(testBuffer);
                    bytesWritten += (long)testBuffer.length;
                }
            }

            diff = bytesWritten;
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
        assertEquals(fileSize,diff);
    }

    private OutputStream creatAppendStream(Path objectPath)throws IOException{
        EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
        flags.add(CreateFlag.APPEND);
        FsPermission permission = new FsPermission((short)00644);
        return fs.create(objectPath,permission,flags,
                fs.getConf().getInt("io.file.buffer.size", 4096),
                fs.getDefaultReplication(objectPath), fs.getDefaultBlockSize(objectPath),(Progressable)null);

    }
}


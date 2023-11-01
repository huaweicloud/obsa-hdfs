package org.apache.hadoop.fs.obs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.EnumSet;

public class ITestOBSCreate {
    private OBSFileSystem fs;

    private static long writeBufferSize = 100 * 1024 * 1024;

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
        fs = OBSTestUtils.createTestFileSystem(conf);
        writeBufferSize = OBSCommonUtils
            .getMultipartSizeProperty(conf, OBSConstants.MULTIPART_SIZE,
                OBSConstants.DEFAULT_MULTIPART_SIZE);
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            OBSFSTestUtil.deletePathRecursive(fs, new Path(testRootPath));
        }
    }

    private Path getTestPath(String relativePath) {
        return new Path(testRootPath + "/" + relativePath);
    }

    @Test
    // 创建一个stream，overwrite为false
    public void testCreateNormal001() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFile = getTestPath("test_file");

        FSDataOutputStream outputStream = null;
        // create with CreateFlag argument
        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }

        OBSFSTestUtil.assertPathExistence(fs, testFile, true);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);

        // create with overwrite argument
        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                false, 4096,
                (short) 3, 128 * 1024 * 1024, null);
            //normal create test case
            String position = "0";
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertPathExistence(fs, testFile, true);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // 创建一个stream，路径为一个存在的文件，overwrite为true
    public void testCreateNormal002() throws Exception {
        Path testFile = getTestPath("test_file");

        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                null, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }

        OBSFSTestUtil.assertPathExistence(fs, testFile, true);

        // overwrite when exist with overwrite argument
        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                true, 4096,
                (short) 3, 128 * 1024 * 1024, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, true);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertPathExistence(fs, testFile, true);

        // overwrite when exist with createflag argument
        EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
        flags.add(CreateFlag.OVERWRITE);
        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, true);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertPathExistence(fs, testFile, true);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // createNonRecursive stream，父目录已存在
    public void testCreateNormal003() throws Exception {
        Path testFile = getTestPath("a001/b001/test_file");
        fs.mkdirs(testFile.getParent());

        byte[] testBuffer = new byte[128];
        for (int i = 0; i < testBuffer.length; ++i) {
            testBuffer[i] = (byte) (i % 128);
        }
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                false, 4096,
                (short) 3, 128 * 1024 * 1024, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
            outputStream.write(testBuffer);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertFileHasLength(fs, testFile, testBuffer.length);

        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.OVERWRITE);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, true);
            outputStream.write(testBuffer);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertFileHasLength(fs, testFile, testBuffer.length);
        OBSFSTestUtil.deletePathRecursive(fs, testFile.getParent());
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // 创建write stream，调用write()，写入100字节
    public void testCreateNormal004() throws Exception {
        Path testFile = getTestPath("a001/b001/test_file");

        byte[] testBuffer = new byte[100];
        for (int i = 0; i < testBuffer.length; ++i) {
            testBuffer[i] = (byte) (i % 128);
        }
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                false, 4096,
                (short) 3, 128 * 1024 * 1024, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
            outputStream.write(testBuffer);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertFileHasLength(fs, testFile, testBuffer.length);

        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.OVERWRITE);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, true);
            outputStream.write(testBuffer);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertFileHasLength(fs, testFile, testBuffer.length);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // 创建write stream，调用write(byte[] source, int offset, int len)，写入100MB
    public void testCreateNormal005() throws Exception {
        Path testFile = getTestPath("a001/b001/test_file");
        FSDataOutputStream outputStream = null;

        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                false, 4096,
                (short) 3, 128 * 1024 * 1024, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
            OBSFSTestUtil.writeData(outputStream, 100 * 1024 * 1024);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertFileHasLength(fs, testFile, 100 * 1024 * 1024);

        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.OVERWRITE);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, true);
            OBSFSTestUtil.writeData(outputStream, 100 * 1024 * 1024);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertFileHasLength(fs, testFile, 100 * 1024 * 1024);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // 创建write stream，调用write(byte[] source, int offset, int len)，写入110MB
    public void testCreateNormal006() throws Exception {
        Path testFile = getTestPath("a001/b001/test_file");
        FSDataOutputStream outputStream = null;

        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                false, 4096,
                (short) 3, 128 * 1024 * 1024, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
            OBSFSTestUtil.writeData(outputStream, 110 * 1024 * 1024);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertFileHasLength(fs, testFile, 110 * 1024 * 1024);

        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.OVERWRITE);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, true);
            OBSFSTestUtil.writeData(outputStream, 110 * 1024 * 1024);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertFileHasLength(fs, testFile, 110 * 1024 * 1024);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // 创建write stream，调用write(byte[] source, int offset, int len)，写入110MB，调用hflush刷数据上云，数据正常。
    // 然后调用write()写100字节，再调用write(byte[] source, int offset, int len)写150MB
    public void testCreateNormal007() throws Exception {
        Path testFile = getTestPath("a001/b001/test_file");
        FSDataOutputStream outputStream = null;

        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                false, 4096,
                (short) 3, 128 * 1024 * 1024, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
            OBSFSTestUtil.writeData(outputStream, 10 * 1024 * 1024);
            outputStream.hflush();
            if (fs.isFsBucket()) {
                OBSFSTestUtil.verifyReceivedData(fs, testFile,
                    10 * 1024 * 1024);
            }

            OBSFSTestUtil.writeData(outputStream, 100);
            OBSFSTestUtil.writeData(outputStream, 15 * 1024 * 1024);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertFileHasLength(fs, testFile,
            10 * 1024 * 1024 + 100 + 15 * 1024 * 1024);

        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.OVERWRITE);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, true);
            OBSFSTestUtil.writeData(outputStream, 10 * 1024 * 1024);
            outputStream.hflush();
            if (fs.isFsBucket()) {
                OBSFSTestUtil.verifyReceivedData(fs, testFile,
                    10 * 1024 * 1024);
            }

            System.out.println(fs.getFileStatus(testFile).getLen());
            OBSFSTestUtil.writeData(outputStream, 100);
            System.out.println(fs.getFileStatus(testFile).getLen());
            OBSFSTestUtil.writeData(outputStream, 15 * 1024 * 1024);
            System.out.println(fs.getFileStatus(testFile).getLen());
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertFileHasLength(fs, testFile,
            10 * 1024 * 1024 + 100 + 15 * 1024 * 1024);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // create的文件不存在，overwrite参数为true，文件创建成功
    public void testCreateNormal009() throws Exception {
        Path testFile = getTestPath("a001/b001/test_file");

        // create with overwrite argument
        boolean hasException = false;
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                true, 4096,
                (short) 3, 128 * 1024 * 1024, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
        } catch (FileNotFoundException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        //        assertEquals("create non exist file with overwrite flag should throw exception.",
        //                true, hasException);

        // create with create flag
        hasException = false;
        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.OVERWRITE);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, true);
        } catch (FileNotFoundException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        //        assertEquals("create non exist file with overwrite flag should throw exception.",
        //                true, hasException);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // 创建write stream，调用write(byte[] source, int offset, int len)，写入110MB，调用hsync刷数据上云，数据正常。
    // 然后调用write()写100字节，再调用write(byte[] source, int offset, int len)写150MB
    public void testCreateNormal008() throws Exception {
        Path testFile = getTestPath("a001/b001/test_file");
        FSDataOutputStream outputStream = null;

        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                false, 4096,
                (short) 3, 128 * 1024 * 1024, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
            OBSFSTestUtil.writeData(outputStream, 110 * 1024 * 1024);
            outputStream.hsync();
            if (fs.isFsBucket()) {
                OBSFSTestUtil.verifyReceivedData(fs, testFile,
                    110 * 1024 * 1024);
            }

            OBSFSTestUtil.writeData(outputStream, 100);
            OBSFSTestUtil.writeData(outputStream, 150 * 1024 * 1024);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertFileHasLength(fs, testFile,
            110 * 1024 * 1024 + 100 + 150 * 1024 * 1024);

        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.OVERWRITE);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, true);
            OBSFSTestUtil.writeData(outputStream, 110 * 1024 * 1024);
            outputStream.hsync();
            if (fs.isFsBucket()) {
                OBSFSTestUtil.verifyReceivedData(fs, testFile,
                    110 * 1024 * 1024);
            }

            OBSFSTestUtil.writeData(outputStream, 100);
            OBSFSTestUtil.writeData(outputStream, 150 * 1024 * 1024);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.assertFileHasLength(fs, testFile,
            110 * 1024 * 1024 + 100 + 150 * 1024 * 1024);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // 测试fs.obs.file.visibility.enable开关有效性
    public void testCreateNormal010() throws Exception {
        // 1、默认不开启fs.obs.file.visibility.enable开关
        Configuration conf = OBSContract.getConfiguration(null);
        fs = OBSTestUtils.createTestFileSystem(conf);
        Path testFile = getTestPath("test_file");
        fs.delete(testFile, true);

        FSDataOutputStream outputStream = fs.create(testFile);
        OBSFSTestUtil.assertPathExistence(fs, testFile, false);
        outputStream.close();
        OBSFSTestUtil.assertPathExistence(fs, testFile, true);
        fs.delete(testFile, true);
        fs.close();

        // 2、打开fs.obs.file.visibility.enable开关后，create接口创建空文件
        conf.setBoolean("fs.obs.file.visibility.enable", true);
        fs = OBSTestUtils.createTestFileSystem(conf);
        outputStream = fs.create(testFile);
        OBSFSTestUtil.assertPathExistence(fs, testFile, true);
        outputStream.close();
        fs.delete(testFile, true);
        fs.close();

        // 3、关闭fs.obs.file.visibility.enable开关后，create接口不会创建空文件
        conf.setBoolean("fs.obs.file.visibility.enable", false);
        fs = OBSTestUtils.createTestFileSystem(conf);
        outputStream = fs.create(testFile);
        OBSFSTestUtil.assertPathExistence(fs, testFile, false);
        outputStream.close();
        OBSFSTestUtil.assertPathExistence(fs, testFile, true);
        fs.delete(testFile, true);
    }

    @Test
    // 文件桶创建一个append stream，路径为一个文件
    public void testCreatePosixNormal001() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFile = getTestPath("a001/b001/test_file");

        FSDataOutputStream outputStream = null;
        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.APPEND);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }

        OBSFSTestUtil.assertPathExistence(fs, testFile, true);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // 文件桶createNonRecursive append stream，写入128字节
    public void testCreatePosixNormal002() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFile = getTestPath("a001/b001/test_file");
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
        fs.mkdirs(testFile.getParent());

        byte[] testBuffer = new byte[128];
        for (int i = 0; i < testBuffer.length; ++i) {
            testBuffer[i] = (byte) (i % 128);
        }
        FSDataOutputStream outputStream = null;
        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.APPEND);
            outputStream = fs.createNonRecursive(testFile,
                new FsPermission((short) 00644), flags, 4096,
                (short) 3, 128 * 1024 * 1024, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
            outputStream.write(testBuffer);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }

        OBSFSTestUtil.assertFileHasLength(fs, testFile, testBuffer.length);
        OBSFSTestUtil.deletePathRecursive(fs, testFile.getParent());
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // 文件桶create append stream, 写入100字节
    public void testCreatePosixNormal003() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFile = getTestPath("a001/b001/test_file");
        OBSFSTestUtil.deletePathRecursive(fs, testFile);

        byte[] testBuffer = new byte[128];
        for (int i = 0; i < testBuffer.length; ++i) {
            testBuffer[i] = (byte) (i % 128);
        }
        FSDataOutputStream outputStream = null;
        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.APPEND);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
            outputStream.write(testBuffer);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }

        OBSFSTestUtil.assertFileHasLength(fs, testFile, testBuffer.length);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // 文件桶create append stream, 写入100M
    public void testCreatePosixNormal004() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFile = getTestPath("a001/b001/test_file");
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
        FSDataOutputStream outputStream = null;

        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.APPEND);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
            OBSFSTestUtil.writeData(outputStream, 100 * 1024 * 1024);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }

        OBSFSTestUtil.assertFileHasLength(fs, testFile, 100 * 1024 * 1024);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // 文件桶create append stream, 写入110M
    public void testCreatePosixNormal005() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFile = getTestPath("a001/b001/test_file");
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
        FSDataOutputStream outputStream = null;

        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.APPEND);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
            OBSFSTestUtil.writeData(outputStream, 110 * 1024 * 1024);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }

        OBSFSTestUtil.assertFileHasLength(fs, testFile, 110 * 1024 * 1024);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // 文件桶create append stream，调用write(byte[] source, int offset, int len)，写入110MB，调用hflush刷数据上云，数据正常。
    // 然后调用write()写100字节，再调用write(byte[] source, int offset, int len)写150MB
    public void testCreatePosixNormal006() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFile = getTestPath("a001/b001/test_file");
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
        FSDataOutputStream outputStream = null;

        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.APPEND);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
            OBSFSTestUtil.writeData(outputStream, 110 * 1024 * 1024);
            outputStream.hflush();
            OBSFSTestUtil.verifyReceivedData(fs, testFile, 110 * 1024 * 1024);

            OBSFSTestUtil.writeData(outputStream, 100);
            OBSFSTestUtil.writeData(outputStream, 150 * 1024 * 1024);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }

        OBSFSTestUtil.assertFileHasLength(fs, testFile,
            110 * 1024 * 1024 + 100 + 150 * 1024 * 1024);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // 文件桶create append stream，调用write(byte[] source, int offset, int len)，写入110MB，调用hsync刷数据上云，数据正常。
    // 然后调用write()写100字节，再调用write(byte[] source, int offset, int len)写150MB
    public void testCreatePosixNormal007() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFile = getTestPath("a001/b001/test_file");
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
        FSDataOutputStream outputStream = null;

        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.APPEND);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
            OBSFSTestUtil.assertPathExistence(fs, testFile, false);
            OBSFSTestUtil.writeData(outputStream, 110 * 1024 * 1024);
            outputStream.hsync();
            OBSFSTestUtil.verifyReceivedData(fs, testFile, 110 * 1024 * 1024);

            OBSFSTestUtil.writeData(outputStream, 100);
            OBSFSTestUtil.writeData(outputStream, 150 * 1024 * 1024);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }

        OBSFSTestUtil.assertFileHasLength(fs, testFile,
            110 * 1024 * 1024 + 100 + 150 * 1024 * 1024);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // create的文件已存在，overwrite参数为false，抛出FileAlreadyExistsException
    public void testCreateAbnormal001() throws Exception {
        Path testFile = getTestPath("a001/b001/test_file");

        FSDataOutputStream outputStream = null;
        try {
            outputStream = OBSFSTestUtil.createStream(fs, testFile, false);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }

        // create with overwrite argument
        boolean hasException = false;
        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                false, 4096,
                (short) 3, 128 * 1024 * 1024, null);
        } catch (FileAlreadyExistsException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertEquals(
            "create exist file with no overwrite flag should throw exception.",
            true, hasException);

        // create with createflag argument
        hasException = false;
        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
        } catch (FileAlreadyExistsException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertEquals(
            "create exist file with no overwrite flag should throw exception.",
            true, hasException);

        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // create的路径是目录，抛出FileAlreadyExistsException
    public void testCreateAbnormal002() throws Exception {
        Path testDir = getTestPath("a001/b001/test_file");
        fs.mkdirs(testDir);

        FSDataOutputStream outputStream = null;
        boolean hasException = false;
        try {
            outputStream = fs.create(testDir, new FsPermission((short) 00644),
                false, 4096,
                (short) 3, 128 * 1024 * 1024, null);
        } catch (FileAlreadyExistsException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        hasException = false;
        try {
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            flags.add(CreateFlag.OVERWRITE);
            outputStream = fs.create(testDir, new FsPermission((short) 00644),
                flags, 4096, (short) 3,
                128 * 1024 * 1024, null, null);
        } catch (FileAlreadyExistsException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertEquals("create stream for folder should throw exception.",
            true, hasException);
        OBSFSTestUtil.deletePathRecursive(fs, testDir);
    }

    @Test
    // createNonRecursive 路径的父目录是文件，抛出ParentNotDirectoryException
    public void testCreateAbnormal003() throws Exception {
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

            outputStream = OBSFSTestUtil.createNonRecursiveStream(fs, testFile);
        } catch (ParentNotDirectoryException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertEquals(
            "create non recursive stream for path whose parent path "
                + "with same file should throw exception.",
            true,
            hasException);
        OBSFSTestUtil.deletePathRecursive(fs, testFile.getParent());
    }

    @Test
    // createNonRecursive 路径的父目录不存在，抛出FileNotFoundException
    public void testCreateAbnormal004() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFile = getTestPath("a001/b001/test_file");

        FSDataOutputStream outputStream = null;
        boolean hasException = false;
        try {
            outputStream = fs.createNonRecursive(testFile,
                new FsPermission((short) 00644),
                EnumSet.noneOf(CreateFlag.class),
                4096, (short) 3, 128 * 1024 * 1024, null);
        } catch (FileNotFoundException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertEquals(
            "create non recursive stream for non exist path should throw exception.",
            true,
            hasException);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // create 携带LAZY_PERSIST、NEW_BLOCK、NO_LOCAL_WRITE、SHOULD_REPLICATE
    // 、IGNORE_CLIENT_LOCALITY，
    // 抛出UnsupportedOperationException
    // hadoop2.8.3 无SHOULD_REPLICATE和IGNORE_CLIENT_LOCALITY flag
    public void testCreateAbnormal005() throws Exception {
        Path testFile = getTestPath("a001/b001/test_file");
        fs.mkdirs(testFile.getParent());

        FSDataOutputStream outputStream = null;
        EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
        boolean hasException;

        // LAZY_PERSIST
        flags.add(CreateFlag.LAZY_PERSIST);
        hasException = false;
        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096,
                (short) 3, 128 * 1024 * 1024, null, null);
        } catch (UnsupportedOperationException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertEquals(
            "create stream with LAZY_PERSIST flag throw UnsupportedOperationException.",
            true,
            hasException);

        hasException = false;
        try {
            outputStream = fs.createNonRecursive(testFile,
                new FsPermission((short) 00644), flags,
                4096, (short) 3, 128 * 1024 * 1024, null);
        } catch (UnsupportedOperationException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertEquals(
            "create stream with LAZY_PERSIST flag throw UnsupportedOperationException.",
            true,
            hasException);
        flags.clear();

        // NEW_BLOCK
        flags.add(CreateFlag.NEW_BLOCK);
        hasException = false;
        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096,
                (short) 3, 128 * 1024 * 1024, null, null);
        } catch (UnsupportedOperationException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertEquals(
            "create stream with NEW_BLOCK flag throw UnsupportedOperationException.",
            true,
            hasException);

        hasException = false;
        try {
            outputStream = fs.createNonRecursive(testFile,
                new FsPermission((short) 00644), flags,
                4096, (short) 3, 128 * 1024 * 1024, null);
        } catch (UnsupportedOperationException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertEquals(
            "create stream with NEW_BLOCK flag throw UnsupportedOperationException.",
            true,
            hasException);
        flags.clear();

        // NO_LOCAL_WRITE
        flags.add(CreateFlag.NO_LOCAL_WRITE);
        hasException = false;
        try {
            outputStream = fs.create(testFile, new FsPermission((short) 00644),
                flags, 4096,
                (short) 3, 128 * 1024 * 1024, null, null);
        } catch (UnsupportedOperationException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertEquals(
            "create stream with NO_LOCAL_WRITE flag throw UnsupportedOperationException.",
            true,
            hasException);

        hasException = false;
        try {
            outputStream = fs.createNonRecursive(testFile,
                new FsPermission((short) 00644), flags,
                4096, (short) 3, 128 * 1024 * 1024, null);
        } catch (UnsupportedOperationException e) {
            hasException = true;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        assertEquals(
            "create stream with NO_LOCAL_WRITE flag throw UnsupportedOperationException.",
            true,
            hasException);
        flags.clear();
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }
}

package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ITestOBSCopyFromLocalFile {
    private OBSFileSystem fs;

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
        conf.setClass(OBSConstants.OBS_METRICS_CONSUMER,
            MockMetricsConsumer.class, BasicMetricsConsumer.class);
        conf.setBoolean(OBSConstants.METRICS_SWITCH, true);
        fs = OBSTestUtils.createTestFileSystem(conf);
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }

    private Path getTestPath(String relativePath) {
        return new Path(testRootPath + "/" + relativePath);
    }

    @Test
    // src是文件，dst是文件，delSrc为true，overwrite为true, dst已存在；src文件大小为0，copy后桶内文件大小也为0
    public void testCopyFromLocalFileNormal001() throws Exception {
        String localFile = "local_file";
        String destFile = "test_file";
        copyFromLocalFile(localFile, destFile, 0);
        assertTrue(fs.getFileStatus(getTestPath(destFile)).getLen() == 0);
        OBSFSTestUtil.deleteLocalFile("local_file");
    }

    @Test
    // src是文件，dst是文件，delSrc为true，overwrite为true, dst已存在；src文件长度小于段大小，为100B，copy后桶内文件大小也为100B
    public void testCopyFromLocalFileNormal002() throws Exception {
        String localFile = "local_file";
        String destFile = "test_file";
        copyFromLocalFile(localFile, destFile, 100);
        assertTrue(fs.getFileStatus(getTestPath(destFile)).getLen() == 100);
        OBSFSTestUtil.deleteLocalFile("local_file");
    }

    @Test
    // src是文件，dst是文件，delSrc为true，overwrite为true, dst已存在；src文件长度小于段大小，为5M，copy后桶内文件大小也为5M
    public void testCopyFromLocalFileNormal003() throws Exception {
        String localFile = "local_file";
        String destFile = "test_file";
        copyFromLocalFile(localFile, destFile, 5 * 1024 * 1024);
        assertEquals(5 * 1024 * 1024,
            fs.getFileStatus(getTestPath(destFile)).getLen());
        OBSFSTestUtil.deleteLocalFile("local_file");
    }

    @Test
    // src是文件，dst是文件，delSrc为true，overwrite为true, dst已存在；src文件长度等于段大小，为100M，copy后桶内文件大小也为100M
    public void testCopyFromLocalFileNormal004() throws Exception {
        String localFile = "local_file";
        String destFile = "test_file";
        copyFromLocalFile(localFile, destFile, 100 * 1024 * 1024);
        assertEquals(100 * 1024 * 1024,
            fs.getFileStatus(getTestPath(destFile)).getLen());
        OBSFSTestUtil.deleteLocalFile("local_file");
    }

    @Test
    // src是文件，dst是文件，delSrc为true，overwrite为true, dst已存在；src文件长度大于段大小，为120M，copy后桶内文件大小也为120M
    public void testCopyFromLocalFileNormal005() throws Exception {
        String localFile = "local_file";
        String destFile = "test_file";
        copyFromLocalFile(localFile, destFile, 120 * 1024 * 1024);
        assertEquals(120 * 1024 * 1024,
            fs.getFileStatus(getTestPath(destFile)).getLen());
        OBSFSTestUtil.deleteLocalFile("local_file");
    }

    @Test
    // src是文件，dst是文件，delSrc为true，overwrite为false, dst不存在
    public void testCopyFromLocalFileNormal006() throws Exception {
        String localFile = "local_file";
        new File(testRootPath, "." + localFile + ".crc").delete();
        OBSFSTestUtil.createLocalTestFile(localFile);

        Path dstPath = getTestPath("test_file");
        OBSFSTestUtil.deletePathRecursive(fs, dstPath);

        fs.copyFromLocalFile(true, false, new Path(localFile), dstPath);
        OBSFSTestUtil.assertPathExistence(fs, dstPath, true);
        OBSFSTestUtil.deleteLocalFile(localFile);
    }

    @Test
    // src是文件，dst是文件，delSrc为true，overwrite为true, dst不存在
    public void testCopyFromLocalFileNormal007() throws Exception {
        String localFile = "local_file";
        new File(testRootPath, "." + localFile + ".crc").delete();
        OBSFSTestUtil.createLocalTestFile(localFile);

        Path dstPath = getTestPath("test_file");
        OBSFSTestUtil.deletePathRecursive(fs, dstPath);

        fs.copyFromLocalFile(false, true, new Path(localFile), dstPath);
        OBSFSTestUtil.assertPathExistence(fs, dstPath, true);
        OBSFSTestUtil.deleteLocalFile(localFile);
    }

    @Test
    // src是文件，dst是根目录，文件拷贝到根目录下
    public void testCopyFromLocalFileNormal008() throws Exception {
        String localFile = "local_file";
        new File(testRootPath, "." + localFile + ".crc").delete();
        OBSFSTestUtil.createLocalTestFile(localFile);

        Path dstPath = new Path("/");
        fs.copyFromLocalFile(false, true, new Path(localFile), dstPath);
        OBSFSTestUtil.assertPathExistence(fs, new Path("/" + localFile), true);

        OBSFSTestUtil.deletePathRecursive(fs, new Path("/" + localFile));
        OBSFSTestUtil.deleteLocalFile(localFile);
    }

    @Test
    // src是文件，dst是多级目录，文件拷贝到多级目录下
    public void testCopyFromLocalFileNormal009() throws Exception {
        String localFile = "local_file";
        new File(testRootPath, "." + localFile + ".crc").delete();
        OBSFSTestUtil.createLocalTestFile(localFile);

        Path dstPath = getTestPath("a001/b001");
        fs.mkdirs(dstPath);
        fs.copyFromLocalFile(false, true, new Path(localFile), dstPath);
        OBSFSTestUtil.assertPathExistence(fs,
            getTestPath("a001/b001/" + localFile), true);

        OBSFSTestUtil.deleteLocalFile(localFile);
    }

    @Test
    // src是空目录，dst是目录，dst不存在，创建dst,将源目录child列表拷贝到dst下
    public void testCopyFromLocalFileNormal010() throws Exception {
        String localDir = "local_dir";
        String localFile = "local_dir/local_file";
        OBSFSTestUtil.createLocalTestDir(localDir);
        new File(testRootPath, "." + localFile + ".crc").delete();
        OBSFSTestUtil.createLocalTestFile(localFile);

        Path dstPath = getTestPath("a001/b001");
        fs.mkdirs(dstPath.getParent());
        fs.copyFromLocalFile(false, true, new Path(localDir), dstPath);
        assertTrue(fs.listStatus(dstPath).length == 1);
        OBSFSTestUtil.deleteLocalFile(localDir);
        fs.delete(dstPath.getParent(), true);
    }

    @Test
    // src是非空目录，dst是目录，dst不存在，创建dst,将源目录child列表拷贝到dst下
    public void testCopyFromLocalFileNormal011() throws Exception {
        String localDir = "local_dir";
        OBSFSTestUtil.createLocalTestDir(localDir);

        Path dstPath = getTestPath("a001/b001");
        fs.mkdirs(dstPath.getParent());
        fs.copyFromLocalFile(false, true, new Path(localDir), dstPath);
        assertTrue(fs.listStatus(dstPath).length == 0);
        OBSFSTestUtil.deleteLocalFile(localDir);
    }

    @Test
    // src是目录，dst是根目录，根目录下不存在与src同名的子目录，正常
    public void testCopyFromLocalFileNormal012() throws Exception {
        String localDir = "local_dir";
        OBSFSTestUtil.createLocalTestDir(localDir);

        Path dstPath = new Path("/");
        fs.mkdirs(dstPath);
        boolean hasException = false;
        try {
            fs.copyFromLocalFile(false, true, new Path(localDir), dstPath);
        } catch (IOException e) {
            hasException = true;
        }
        assertFalse(hasException);

        OBSFSTestUtil.deleteLocalFile(localDir);
        fs.delete(new Path("/local_dir"), true);
    }

    @Test
    // src是目录，dst是多级空目录，dst已存在，正常
    public void testCopyFromLocalFileNormal013() throws Exception {
        String localDir = "local_dir";
        OBSFSTestUtil.createLocalTestDir(localDir);

        Path dstPath = getTestPath("a001/b001");
        fs.mkdirs(dstPath);
        boolean hasException = false;
        try {
            fs.copyFromLocalFile(false, true, new Path(localDir), dstPath);
        } catch (IOException e) {
            hasException = true;
        }
        assertFalse(hasException);
        OBSFSTestUtil.deleteLocalFile(localDir);
    }

    @Test
    // src是文件，dst是文件，overwrite为false，dst已存在，抛FileAlreadyExistsException
    public void testCopyFromLocalFileAbnormal001() throws Exception {
        String localFile = "local_file";
        new File(testRootPath, "." + localFile + ".crc").delete();
        OBSFSTestUtil.createLocalTestFile(localFile);

        Path dstPath = getTestPath("test_file");
        OBSFSTestUtil.deletePathRecursive(fs, dstPath);
        FSDataOutputStream outputStream = fs.create(dstPath, false);
        outputStream.close();
        OBSFSTestUtil.assertPathExistence(fs, dstPath, true);

        boolean hasException = false;
        try {
            fs.copyFromLocalFile(false, false, new Path(localFile), dstPath);
        } catch (IOException e) {
            hasException = true;
        }
        assertTrue(hasException);

        fs.delete(dstPath, true);
        OBSFSTestUtil.deleteLocalFile(localFile);
    }

    @Test
    // src不存在，抛FileNotFoundException
    public void testCopyFromLocalFileAbnormal002() throws Exception {
        String localFile = "local_file";
        OBSFSTestUtil.deleteLocalFile(localFile);

        Path dstPath = getTestPath("test_file");

        boolean hasException = false;
        try {
            fs.copyFromLocalFile(false, true, new Path(localFile), dstPath);
        } catch (FileNotFoundException e) {
            hasException = true;
        }
        assertTrue(hasException);

        fs.delete(dstPath, true);
        OBSFSTestUtil.deleteLocalFile(localFile);
    }

    @Test
    // 测试基类copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst)方法
    public void testCplParentMethod01() throws Exception {
        String localFile1 = "local_file1";
        String localFile2 = "local_file2";
        new File(testRootPath, "." + localFile1 + ".crc").delete();
        new File(testRootPath, "." + localFile2 + ".crc").delete();
        OBSFSTestUtil.createLocalTestFile(localFile1);
        OBSFSTestUtil.createLocalTestFile(localFile2);
        Path[] srcFiles = {new Path(localFile1), new Path(localFile2)};

        Path dstDir = getTestPath("test_dir");
        fs.mkdirs(dstDir);
        fs.copyFromLocalFile(true, false, srcFiles, dstDir);
        OBSFSTestUtil.assertPathExistence(fs,
            getTestPath("test_dir/local_file1"), true);
        OBSFSTestUtil.assertPathExistence(fs,
            getTestPath("test_dir/local_file2"), true);

        fs.delete(getTestPath("test_dir"), true);
        OBSFSTestUtil.deleteLocalFile(localFile1);
        OBSFSTestUtil.deleteLocalFile(localFile2);
    }

    @Test
    // 测试基类copyFromLocalFile(boolean delSrc, Path src, Path dst)方法，overwrite始终为true
    public void testCplParentMethod02() throws Exception {
        String localFile = "local_file";
        new File(testRootPath, "." + localFile + ".crc").delete();
        File file = OBSFSTestUtil.createLocalTestFile(localFile);
        OBSFSTestUtil.writeLocalFile(file, 16);

        String dstFile = "test_file";
        FSDataOutputStream outputStream = fs.create(getTestPath(dstFile),
            false);
        outputStream.close();

        fs.copyFromLocalFile(true, new Path(localFile), getTestPath(dstFile));
        assertEquals(16, fs.getFileStatus(getTestPath(dstFile)).getLen());
        assertFalse(file.exists());

        fs.delete(getTestPath(dstFile), true);
    }

    @Test
    // 测试基类copyFromLocalFile(Path src, Path dst)方法, overwrite为true, desSrc为false
    public void testCplParentMethod03() throws Exception {
        String localFile = "local_file";
        new File(testRootPath, "." + localFile + ".crc").delete();
        File file = OBSFSTestUtil.createLocalTestFile(localFile);
        OBSFSTestUtil.writeLocalFile(file, 16);

        String dstFile = "test_file";
        FSDataOutputStream outputStream = fs.create(getTestPath(dstFile),
            false);
        outputStream.close();

        fs.copyFromLocalFile(new Path(localFile), getTestPath(dstFile));
        assertEquals(16, fs.getFileStatus(getTestPath(dstFile)).getLen());
        assertTrue(file.exists());

        fs.delete(getTestPath(dstFile), true);
        OBSFSTestUtil.deleteLocalFile(localFile);
    }

    @Test
    // 测试基类moveFromLocalFile(Path[] srcs, Path dst)方法，执行copyFromLocalFile(true, true, srcs, dst)
    public void testCplParentMethod04() throws Exception {
        String localFile1 = "local_file1";
        String localFile2 = "local_file2";
        new File(testRootPath, "." + localFile1 + ".crc").delete();
        new File(testRootPath, "." + localFile2 + ".crc").delete();
        File file1 = OBSFSTestUtil.createLocalTestFile(localFile1);
        File file2 = OBSFSTestUtil.createLocalTestFile(localFile2);
        OBSFSTestUtil.writeLocalFile(file1, 16);
        OBSFSTestUtil.writeLocalFile(file2, 16);
        Path[] srcFiles = {new Path(localFile1), new Path(localFile2)};

        String dstDir = "dest_dir";
        fs.mkdirs(getTestPath(dstDir));

        fs.moveFromLocalFile(srcFiles, getTestPath(dstDir));
        OBSFSTestUtil.assertPathExistence(fs,
            getTestPath("dest_dir/local_file1"), true);
        OBSFSTestUtil.assertPathExistence(fs,
            getTestPath("dest_dir/local_file2"), true);
        assertFalse(file1.exists());
        assertFalse(file2.exists());

        fs.delete(getTestPath(dstDir), true);
    }

    @Test
    // 测试基类moveFromLocalFile(Path src, Path dst)方法，执行copyFromLocalFile(delSrc = true, src, dst)
    public void testCplParentMethod05() throws Exception {
        String localFile = "local_file";
        new File(testRootPath, "." + localFile + ".crc").delete();
        File file = OBSFSTestUtil.createLocalTestFile(localFile);
        OBSFSTestUtil.writeLocalFile(file, 16);

        String dstFile = "test_file";
        FSDataOutputStream outputStream = fs.create(getTestPath(dstFile),
            false);
        outputStream.close();

        fs.moveFromLocalFile(new Path(localFile), getTestPath(dstFile));
        assertEquals(16, fs.getFileStatus(getTestPath(dstFile)).getLen());
        assertFalse(file.exists());

        fs.delete(getTestPath(dstFile), true);
    }

    @Test
    // 测试基类copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem)方法
    // overwrite始终为true
    public void testCplParentMethod06() throws Exception {
        String localFile = "local_file";
        new File(testRootPath, "." + localFile + ".crc").delete();
        File file = OBSFSTestUtil.createLocalTestFile(localFile);
        OBSFSTestUtil.writeLocalFile(file, 16);

        String srcFile = "test_file";
        FSDataOutputStream outputStream = fs.create(getTestPath(srcFile),
            false);
        outputStream.close();

        fs.copyToLocalFile(true, getTestPath(srcFile), new Path(localFile),
            true);
        assertEquals(0, file.length());
        OBSFSTestUtil.assertPathExistence(fs, getTestPath(srcFile), false);

        OBSFSTestUtil.deleteLocalFile(localFile);
        fs.delete(getTestPath(srcFile), true);
    }

    @Test
    // 测试基类copyToLocalFile(boolean delSrc, Path src, Path dst)方法，useRawLocalFileSystem为false
    public void testCplParentMethod07() throws Exception {
        String localFile = "local_file";
        new File(testRootPath, "." + localFile + ".crc").delete();
        File file = OBSFSTestUtil.createLocalTestFile(localFile);
        OBSFSTestUtil.writeLocalFile(file, 16);

        String srcFile = "test_file";
        FSDataOutputStream outputStream = fs.create(getTestPath(srcFile),
            false);
        outputStream.close();

        fs.copyToLocalFile(true, getTestPath(srcFile), new Path(localFile));
        assertEquals(0, file.length());
        OBSFSTestUtil.assertPathExistence(fs, getTestPath(srcFile), false);

        OBSFSTestUtil.deleteLocalFile(localFile);
        fs.delete(getTestPath(srcFile), true);
    }

    @Test
    // 测试基类copyToLocalFile(Path src, Path dst)方法, delSrc为false，useRawLocalFileSystem为false
    public void testCplParentMethod08() throws Exception {
        String localFile = "local_file";
        new File(testRootPath, "." + localFile + ".crc").delete();
        File file = OBSFSTestUtil.createLocalTestFile(localFile);
        OBSFSTestUtil.writeLocalFile(file, 16);

        String srcFile = "test_file";
        FSDataOutputStream outputStream = fs.create(getTestPath(srcFile),
            false);
        outputStream.close();

        fs.copyToLocalFile(getTestPath(srcFile), new Path(localFile));
        assertEquals(0, file.length());
        OBSFSTestUtil.assertPathExistence(fs, getTestPath(srcFile), true);

        OBSFSTestUtil.deleteLocalFile(localFile);
        fs.delete(getTestPath(srcFile), true);
    }

    @Test
    // 测试基类moveToLocalFile(Path src, Path dst)方法, delSrc为true, useRawLocalFileSystem为false
    public void testCplParentMethod09() throws Exception {
        String localFile = "local_file";
        new File(testRootPath, "." + localFile + ".crc").delete();
        File file = OBSFSTestUtil.createLocalTestFile(localFile);
        OBSFSTestUtil.writeLocalFile(file, 16);

        String srcFile = "test_file";
        FSDataOutputStream outputStream = fs.create(getTestPath(srcFile),
            false);
        outputStream.close();

        fs.moveToLocalFile(getTestPath(srcFile), new Path(localFile));
        assertEquals(0, file.length());
        OBSFSTestUtil.assertPathExistence(fs, getTestPath(srcFile), false);

        OBSFSTestUtil.deleteLocalFile(localFile);
        fs.delete(getTestPath(srcFile), true);
    }

    private void copyFromLocalFile(String srcFile, String destFile,
        long srcFileSize) throws IOException {
        Path localPath = new Path(srcFile);
        new File(testRootPath, "." + srcFile + ".crc").delete();
        File localFile = OBSFSTestUtil.createLocalTestFile(srcFile);
        OBSFSTestUtil.writeLocalFile(localFile, srcFileSize);

        Path dstPath = getTestPath(destFile);
        fs.delete(dstPath, true);
        FSDataOutputStream outputStream = OBSFSTestUtil.createStream(fs,
            dstPath);
        outputStream.close();

        fs.copyFromLocalFile(true, true, localPath, dstPath);
        OBSFSTestUtil.assertPathExistence(fs, dstPath, true);
    }
}

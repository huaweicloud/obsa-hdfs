package org.apache.hadoop.fs.obs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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

public class ITestOBSListStatus {
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
        conf.set(OBSConstants.MULTIPART_SIZE, String.valueOf(5 * 1024 * 1024));
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
    // 路径是根目录，FileStatus结果数组长度大于等于0
    public void testListStatus001() throws IOException {
        FileStatus[] summaries = fs.listStatus(new Path("/"));
        assertTrue(summaries.length >= 0);
    }

    @Test
    // 路径是一个文件，FileStatus结果数组长度等于1，且为文件
    public void testListStatus002() throws IOException {
        Path testFile = getTestPath("test_file");
        fs.delete(testFile, true);
        FSDataOutputStream outputStream = fs.create(testFile, false);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        outputStream.write(data);
        outputStream.close();

        FileStatus[] status = fs.listStatus(testFile);
        assertEquals(1, status.length);
        assertTrue(status[0].isFile());
        if (fs.exists(testFile)) {
            fs.delete(testFile, true);
        }
    }

    @Test
    // 路径是一个空目录，FileStatus结果数组长度等于0
    public void testListStatus003() throws Exception {
        Path testFolder = getTestPath("test_dir");
        fs.delete(testFolder, true);
        fs.mkdirs(testFolder);
        FileStatus[] status = fs.listStatus(testFolder);
        assertEquals(0, status.length);
    }

    @Test
    // 路径是一个非空目录，FileStatus结果数组长度等于子目录数和子文件数之和
    public void testListStatus004() throws IOException {
        Path testFolder = getTestPath("test_dir");
        fs.mkdirs(testFolder);

        for (int i = 0; i < 2; i++) {
            Path subFolder = getTestPath("test_dir" + "/sub_folder" + i);
            fs.mkdirs(subFolder);
        }

        byte[] data = ContractTestUtils.dataset(8, 'a', 26);
        for (int i = 0; i < 3; i++) {
            Path file = getTestPath("test_dir" + "/sub_file" + i);
            FSDataOutputStream outputStream = fs.create(file, true);
            outputStream.write(data);
            outputStream.close();
        }

        FileStatus[] status = fs.listStatus(testFolder);
        System.out.println(
            "folder " + testFolder + " size: " + status.length);

        assertEquals(2 + 3, status.length);
        if (fs.exists(testFolder)) {
            fs.delete(testFolder, true);
        }
    }

    @Test
    // 文件不存在，抛FileNotFoundException
    public void testListStatus005() throws IOException {
        Path testFolder = getTestPath("test_file");
        if (fs.exists(testFolder)) {
            fs.delete(testFolder, true);
        }

        boolean hasException = false;
        try {
            fs.listStatus(testFolder);
        } catch (FileNotFoundException e) {
            hasException = true;
        }
        assertTrue(hasException);
    }

    @Test
    // 文件的父目录及上级目录不存在，抛FileNotFoundException
    public void testListStatus006() throws IOException {
        Path testFolder = getTestPath("test_file");
        if (fs.exists(testFolder)) {
            fs.delete(testFolder, true);
        }

        boolean hasException = false;
        try {
            fs.listStatus(testFolder);
        } catch (FileNotFoundException e) {
            hasException = true;
        }
        assertTrue(hasException);
    }

    @Test
    // 路径的父目录及上级目录不是一个目录，抛AccessControlException
    public void testListStatus007() throws IOException {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFile = getTestPath("a001/b001/test_file");
        fs.delete(testFile.getParent().getParent(), true);

        FSDataOutputStream outputStream = fs.create(testFile.getParent(),
            false);
        outputStream.close();

        boolean hasException = false;
        try {
            fs.listStatus(testFile);
        } catch (AccessControlException e) {
            hasException = true;
        }
        assertTrue(hasException);
    }

    @Test
    // 路径为null, 抛NullPointerException
    public void testListStatus008() throws IOException {
        Path testFolder = null;

        boolean hasException = false;
        try {
            fs.listStatus(testFolder);
        } catch (NullPointerException e) {
            hasException = true;
        }
        assertTrue(hasException);
    }

    @Test
    // 测试基类方法listStatus(Path f, PathFilter filter)，用filter对返回结果列表进行过滤
    public void testParentMethod01() throws Exception {
        Path testPath = getTestPath("test_file");
        final Path qualifiedPath = testPath.makeQualified(fs.getUri(),
            fs.getWorkingDirectory());
        FSDataOutputStream outputStream = fs.create(qualifiedPath, false);
        outputStream.close();

        PathFilter filter = path -> {
            if (path.equals(qualifiedPath)) {
                return true;
            }
            return false;
        };

        FileStatus[] files = fs.listStatus(new Path(testRootPath), filter);
        assertEquals(1, files.length);
        assertTrue(files[0].getPath().equals(qualifiedPath));

        fs.delete(qualifiedPath, true);
    }

    @Test
    // 测试基类方法listStatus(Path[] files, PathFilter filter)，列举多个路径，用filter对返回结果列表进行过滤
    public void testParentMethod02() throws Exception {
        Path dir1 = getTestPath("dir1");
        fs.mkdirs(dir1);
        final Path file1 =
            getTestPath("dir1/test_file1").makeQualified(fs.getUri(),
                fs.getWorkingDirectory());
        FSDataOutputStream outputStream = fs.create(file1, false);
        outputStream.close();

        Path dir2 = getTestPath("dir2");
        fs.mkdirs(dir2);
        final Path file2 =
            getTestPath("dir2/test_file2").makeQualified(fs.getUri(),
                fs.getWorkingDirectory());
        ;
        outputStream = fs.create(file2, false);
        outputStream.close();

        PathFilter filter = path -> {
            if (path.equals(file1) || path.equals(file2)) {
                return true;
            }
            return false;
        };

        Path[] dirs = {dir1, dir2};
        FileStatus[] files = fs.listStatus(dirs, filter);
        assertEquals(2, files.length);
        assertTrue(files[0].getPath().equals(file1));
        assertTrue(files[1].getPath().equals(file2));

        fs.delete(dir1, true);
        fs.delete(dir2, true);
    }

    @Test
    // 测试基类方法listStatus(Path[] files)，列举多个路径，不对返回结果列表进行过滤
    public void testParentMethod03() throws Exception {
        Path dir1 = getTestPath("dir1");
        fs.mkdirs(dir1);
        final Path file1 =
            getTestPath("dir1/test_file1").makeQualified(fs.getUri(),
                fs.getWorkingDirectory());
        FSDataOutputStream outputStream = fs.create(file1, false);
        outputStream.close();

        Path dir2 = getTestPath("dir2");
        fs.mkdirs(dir2);
        final Path file2 =
            getTestPath("dir2/test_file2").makeQualified(fs.getUri(),
                fs.getWorkingDirectory());
        outputStream = fs.create(file2, false);
        outputStream.close();

        Path[] dirs = {dir1, dir2};
        FileStatus[] files = fs.listStatus(dirs);
        assertEquals(2, files.length);
        assertTrue(files[0].getPath().equals(file1));
        assertTrue(files[1].getPath().equals(file2));

        fs.delete(dir1, true);
        fs.delete(dir2, true);
    }

    @Test
    // 测试基类方法listStatusIterator(final Path p)，列举多个路径，不对返回结果列表进行过滤
    public void testParentMethod04() throws Exception {
        Path testFolder = getTestPath("test_dir");
        fs.mkdirs(testFolder);

        for (int i = 0; i < 2; i++) {
            Path subFolder = getTestPath("test_dir" + "/sub_folder" + i);
            fs.mkdirs(subFolder);
        }

        byte[] data = ContractTestUtils.dataset(8, 'a', 26);
        for (int i = 0; i < 3; i++) {
            Path file = getTestPath("test_dir" + "/sub_file" + i);
            FSDataOutputStream outputStream = fs.create(file, true);
            outputStream.write(data);
            outputStream.close();
        }

        FileStatus[] status = fs.listStatus(testFolder);
        System.out.println(
            "folder " + testFolder + " size: " + status.length);

        assertEquals(2 + 3, status.length);

        if (fs.exists(testFolder)) {
            fs.delete(testFolder, true);
        }
    }
}

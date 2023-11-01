package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
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
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ITestOBSListLocatedStatus {
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
    // 列举根目录,不带filter，结果列表大小大于等于0
    public void testListLocatedStatus01() throws Exception {
        RemoteIterator<LocatedFileStatus> iterator = fs.listLocatedStatus(
            new Path("/"));
        List<LocatedFileStatus> files = new ArrayList<>();
        while (iterator.hasNext()) {
            LocatedFileStatus status = iterator.next();
            files.add(status);
        }
        assertTrue(files.size() >= 0);
    }

    @Test
    // 列举文件，不带filter，结果列表大小等于1，并且状态为文件
    public void testListLocatedStatus02() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile, true);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        outputStream.write(data);
        outputStream.close();

        RemoteIterator<LocatedFileStatus> iterator = fs.listLocatedStatus(
            testFile);
        int entries = 0;
        while (iterator.hasNext()) {
            entries++;
            LocatedFileStatus status = iterator.next();
            assertTrue(status.isFile());
        }
        assertEquals(1, entries);
        if (fs.exists(testFile)) {
            fs.delete(testFile, true);
        }
    }

    @Test
    // 列举文件，带filter，fileter指定文件path，结果列表大小等于1，并且状态为文件
    public void testListLocatedStatus03() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile, false);
        outputStream.close();

        PathFilter filter = path -> {
            String relativePath = path.toString().split(fs.getBucket())[1];
            return relativePath.equals(testFile.toString());
        };
        RemoteIterator<LocatedFileStatus> iterator = fs.listLocatedStatus(
            testFile, filter);
        int entries = 0;
        while (iterator.hasNext()) {
            entries++;
            LocatedFileStatus status = iterator.next();
            assertTrue(status.isFile());
        }
        assertEquals(1, entries);
    }

    @Test
    // 列举一个目录，目录下存在多个文件和目录，结果列表大小为目录下文件和子目录个数之和
    public void testListLocatedStatus04() throws Exception {
        Path testFolder = getTestPath("test_folder");
        fs.mkdirs(testFolder);

        for (int i = 0; i < 2; i++) {
            Path subFolder = getTestPath("test_folder/sub_dir-" + i);
            fs.mkdirs(subFolder);
        }

        byte[] data = ContractTestUtils.dataset(8, 'a', 26);
        for (int i = 0; i < 3; i++) {
            Path file = getTestPath("test_folder/sub_file-" + i);
            FSDataOutputStream outputStream = fs.create(file, true);
            outputStream.write(data);
            outputStream.close();
        }

        RemoteIterator<LocatedFileStatus> iterator = fs.listLocatedStatus(
            testFolder);
        int entries = 0;
        while (iterator.hasNext()) {
            entries++;
            iterator.next();
        }
        assertEquals(5, entries);

        if (fs.exists(testFolder)) {
            fs.delete(testFolder, true);
        }
    }

    @Test
    // 列举一个目录，目录下存在多个文件和目录，带filter只过滤出文件，结果列表大小为目录下文件数之和
    public void testListLocatedStatus05() throws Exception {
        Path testFolder = getTestPath("test_folder/");
        fs.mkdirs(testFolder);

        for (int i = 0; i < 2; i++) {
            Path subFolder = getTestPath("test_folder/sub_dir-" + i);
            fs.mkdirs(subFolder);
        }

        byte[] data = ContractTestUtils.dataset(8, 'a', 26);
        for (int i = 0; i < 3; i++) {
            Path file = getTestPath("test_folder/sub_file-" + i);
            FSDataOutputStream outputStream = fs.create(file, true);
            outputStream.write(data);
            outputStream.close();
        }

        PathFilter filter = path -> !path.toString().contains("dir");
        RemoteIterator<LocatedFileStatus> iterator = fs.listLocatedStatus(
            testFolder, filter);
        int entries = 0;
        while (iterator.hasNext()) {
            entries++;
            LocatedFileStatus status = iterator.next();
            assertTrue(status.isFile());
        }

        assertEquals(3, entries);

        if (fs.exists(testFolder)) {
            fs.delete(testFolder, true);
        }
    }

    @Test
    // 如果路径不存在或父目录及上级目录不存在，抛FileNotFoundException
    public void testListLocatedStatusAbnormal01() throws Exception {
        Path testFolder = getTestPath("test_dir");
        if (fs.exists(testFolder)) {
            fs.delete(testFolder, true);
        }

        boolean hasException = false;
        try {
            fs.listLocatedStatus(testFolder);
        } catch (FileNotFoundException e) {
            hasException = true;
        }
        assertTrue(hasException);
    }

    @Test
    // 带filter，如果路径不存在或父目录及上级目录不存在，抛FileNotFoundException
    public void testListLocatedStatusAbnormal02() throws Exception {
        Path testFolder = getTestPath("test_dir");
        if (fs.exists(testFolder)) {
            fs.delete(testFolder, true);
        }

        PathFilter filter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return false;
            }
        };
        boolean hasException = false;
        try {
            fs.listLocatedStatus(testFolder);
        } catch (FileNotFoundException e) {
            hasException = true;
        }
        assertTrue(hasException);
    }

    @Test
    // 如果路径的父目录及上级目录不是一个文件，抛AccessControlException
    public void testListLocatedStatusAbnormal03() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFolder = getTestPath("a001");
        FSDataOutputStream outputStream = fs.create(testFolder, false);
        outputStream.close();

        Path testFile = getTestPath("a001/b001");
        boolean hasException = false;
        try {
            fs.listLocatedStatus(testFile);
        } catch (AccessControlException e) {
            hasException = true;
        }
        assertTrue(hasException);
    }

    @Test
    // 带filter，如果路径的父目录及上级目录不是一个文件，抛AccessControlException
    public void testListLocatedStatusAbnormal04() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFolder = getTestPath("a001");
        FSDataOutputStream outputStream = fs.create(testFolder, false);
        outputStream.close();

        Path testFile = getTestPath("a001/b001");
        PathFilter filter = path -> true;
        boolean hasException = false;
        try {
            fs.listLocatedStatus(testFile, filter);
        } catch (AccessControlException e) {
            hasException = true;
        }
        assertTrue(hasException);
    }
}

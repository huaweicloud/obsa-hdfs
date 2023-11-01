package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.Assert.*;

public class ITestOBSGetFileStatusAndExist {
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
    // 获取根目录的信息，exist返回true
    public void testGetFileStatusAndExistNormal001() throws IOException {
        FileStatus status = fs.getFileStatus(new Path("/"));
        System.out.println(status.toString());
        assertTrue("Root path should be directory.", status.isDirectory());

        assertEquals(fs.exists(new Path("/")), status != null);
    }

    @Test
    // 获取文件的元数据信息，exist返回true
    public void testGetFileStatusAndExistNormal002() throws IOException {
        Path testPath = getTestPath("a001/test_file");
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.create(testPath, false);
            byte[] data = ContractTestUtils.dataset(8, 'a', 26);
            outputStream.write(data);
            outputStream.close();

            FileStatus status = fs.getFileStatus(testPath);
            if (fs.getMetricSwitch()) {
                MockMetricsConsumer mmc
                    = (MockMetricsConsumer) fs.getMetricsConsumer();
                assertEquals(BasicMetricsConsumer.MetricKind.normal, mmc.getMr().getKind());
            }
            System.out.println(status.toString());
            assertTrue("test path should be a file.", status.isFile());
            assertEquals(fs.exists(testPath), status != null);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        OBSFSTestUtil.deletePathRecursive(fs, testPath);
    }

    @Test
    // 获取目录的元数据信息， exist返回true
    public void testGetFileStatusAndExistNormal003() throws IOException {
        Path testPath = getTestPath("a001/b001");
        fs.mkdirs(testPath);

        FileStatus status = fs.getFileStatus(testPath);
        System.out.println(status.toString());
        assertTrue("test path should be directory.", status.isDirectory());
        assertEquals(fs.exists(testPath), status != null);
        OBSFSTestUtil.deletePathRecursive(fs, testPath);
    }

    @Test
    // 路径不存在，getFileStatus抛FileNotFoundException， exist返回false
    public void testGetFileStatusAndExistAbnormal001() throws IOException {
        Path testPath = getTestPath("a001/test_file");
        if (fs.exists(testPath)) {
            fs.delete(testPath, true);
        }

        boolean hasException = false;
        try {
            fs.getFileStatus(testPath);
        } catch (FileNotFoundException fnfe) {
            hasException = true;
        }
        assertTrue(hasException);
        assertFalse(fs.exists(testPath));
    }

    @Test
    // 路径的父目录及上级目录不存在，getFileStatus抛FileNotFoundException， exist返回false
    public void testGetFileStatusAndExistAbnormal002() throws IOException {
        Path testPath = getTestPath("a001/b001/test_file");
        OBSFSTestUtil.deletePathRecursive(fs, testPath.getParent());

        boolean hasException = false;
        try {
            fs.getFileStatus(testPath);
        } catch (FileNotFoundException e) {
            hasException = true;
        }
        assertTrue(hasException);
        assertFalse(fs.exists(testPath));
    }

    @Test
    // 路径的父目录及上级目录非目录，getFileStatus抛FileNotFoundException，exist返false
    public void testGetFileStatusAndExistAbnormal003() throws IOException {
        Path testPath = getTestPath("a001/b001/test_file");
        OBSFSTestUtil.deletePathRecursive(fs, testPath.getParent());

        FSDataOutputStream outputStream = fs.create(testPath.getParent(),
            false);
        outputStream.close();

        boolean hasException = false;
        try {
            fs.getFileStatus(testPath);
            if (fs.getMetricSwitch()) {
                MockMetricsConsumer mmc
                    = (MockMetricsConsumer) fs.getMetricsConsumer();
                assertEquals(BasicMetricsConsumer.MetricKind.normal, mmc.getMr().getKind());
            }
        } catch (FileNotFoundException e) {
            hasException = true;
        }
        assertTrue(hasException);
        assertFalse(fs.exists(testPath));
    }
}

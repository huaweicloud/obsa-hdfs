package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ITestOBSOpen {
    private OBSFileSystem fs;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private static final String TEST_FILE = "dest_file";

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

        OBSFSTestUtil.deletePathRecursive(fs, getTestPath(TEST_FILE));
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
    // open一个文件发起range读，seek到开始读一段
    public void testOpenNormal001() throws Exception {
        Path testFile = getTestPath(TEST_FILE);
        FSDataOutputStream outputStream = fs.create(testFile, true);
        OBSFSTestUtil.writeData(outputStream, 20 * 1024 * 1024);
        outputStream.close();
        OBSFSTestUtil.assertFileHasLength(fs, testFile, 20 * 1024 * 1024);

        boolean hasException = false;
        try {
            OBSFSTestUtil.readFile(fs, testFile, 0, 5 * 1024 * 1024);
        } catch (IOException e) {
            hasException = true;
        }

        assertFalse(hasException);
        OBSFSTestUtil.deletePathRecursive(fs, getTestPath(TEST_FILE));
    }

    @Test
    // open一个文件发起range读，seek到开始读全部
    public void testOpenNormal002() throws Exception {
        Path testFile = getTestPath(TEST_FILE);
        FSDataOutputStream outputStream = fs.create(testFile, true);
        OBSFSTestUtil.writeData(outputStream, 20 * 1024 * 1024);
        outputStream.close();
        OBSFSTestUtil.assertFileHasLength(fs, testFile, 20 * 1024 * 1024);

        boolean hasException = false;
        try {
            OBSFSTestUtil.readFile(fs, testFile, 0, 20 * 1024 * 1024);
        } catch (IOException e) {
            hasException = true;
        }

        assertFalse(hasException);
        OBSFSTestUtil.deletePathRecursive(fs, getTestPath(TEST_FILE));
    }

    @Test
    // open一个文件发起range读，seek到中间读一段
    public void testOpenNormal003() throws Exception {
        Path testFile = getTestPath(TEST_FILE);
        FSDataOutputStream outputStream = fs.create(testFile, true);
        OBSFSTestUtil.writeData(outputStream, 20 * 1024 * 1024);
        outputStream.close();
        OBSFSTestUtil.assertFileHasLength(fs, testFile, 20 * 1024 * 1024);

        boolean hasException = false;
        try {
            OBSFSTestUtil.readFile(fs, testFile, 10 * 1024 * 1024,
                5 * 1024 * 1024);
        } catch (IOException e) {
            hasException = true;
        }

        assertFalse(hasException);
        OBSFSTestUtil.deletePathRecursive(fs, getTestPath(TEST_FILE));
    }

    @Test
    // open一个文件发起range读，seek到中间读至末尾
    public void testOpenNormal004() throws Exception {
        Path testFile = getTestPath(TEST_FILE);
        FSDataOutputStream outputStream = fs.create(testFile, true);
        OBSFSTestUtil.writeData(outputStream, 20 * 1024 * 1024);
        outputStream.close();
        OBSFSTestUtil.assertFileHasLength(fs, testFile, 20 * 1024 * 1024);

        boolean hasException = false;
        try {
            OBSFSTestUtil.readFile(fs, testFile, 10 * 1024 * 1024,
                10 * 1024 * 1024);
        } catch (IOException e) {
            hasException = true;
        }

        assertFalse(hasException);
        OBSFSTestUtil.deletePathRecursive(fs, getTestPath(TEST_FILE));
    }

    @Test
    // open的文件不存在，抛FileNotFoundException
    public void testOpenAbnormal001() throws Exception {
        Path testFile = getTestPath(TEST_FILE);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
        OBSFSTestUtil.assertPathExistence(fs, testFile, false);

        boolean hasException = false;
        FSDataInputStream inputStream = null;
        try {
            inputStream = fs.open(testFile, 4096);
        } catch (FileNotFoundException e) {
            hasException = true;
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }

        assertTrue(hasException);
        OBSFSTestUtil.deletePathRecursive(fs, getTestPath(TEST_FILE));
    }

    @Test
    // open的是一个目录，抛FileNotFoundException
    public void testOpenAbnormal002() throws Exception {
        Path testDir = getTestPath("a/b/c");
        fs.mkdirs(testDir);
        OBSFSTestUtil.assertPathExistence(fs, testDir, true);

        boolean hasException = false;
        FSDataInputStream inputStream = null;
        try {
            inputStream = fs.open(testDir, 4096);
        } catch (FileNotFoundException e) {
            hasException = true;
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }

        assertTrue(hasException);
        OBSFSTestUtil.deletePathRecursive(fs, getTestPath("a/b/c"));
    }

    @Test
    // open的文件父目录及上级目录不存在，抛FileNotFoundException
    public void testOpenAbnormal003() throws Exception {
        Path testPath = getTestPath("a/b/c/d");
        OBSFSTestUtil.deletePathRecursive(fs, testPath.getParent().getParent());
        OBSFSTestUtil.assertPathExistence(fs, testPath.getParent().getParent(),
            false);

        boolean hasException = false;
        FSDataInputStream inputStream = null;
        try {
            inputStream = fs.open(testPath, 4096);
        } catch (FileNotFoundException e) {
            hasException = true;
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }

        assertTrue(hasException);
    }

    @Test
    // open的文件父目录及上级目录不是一个目录，抛AccessControlException
    public void testOpenAbnormal004() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testPath = getTestPath("a/b/c/d");
        fs.mkdirs(testPath.getParent().getParent());
        FSDataOutputStream os = OBSFSTestUtil.createStream(fs,
            testPath.getParent(), true);
        os.close();

        boolean hasException = false;
        FSDataInputStream inputStream = null;
        try {
            inputStream = fs.open(testPath, 4096);
        } catch (AccessControlException e) {
            hasException = true;
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }

        assertTrue(hasException);
        OBSFSTestUtil.deletePathRecursive(fs, testPath.getParent().getParent());
    }
}

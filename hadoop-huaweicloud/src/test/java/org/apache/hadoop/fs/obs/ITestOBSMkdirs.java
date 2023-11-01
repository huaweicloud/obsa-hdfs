package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ITestOBSMkdirs {
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

        fs.delete(new Path(testRootPath), true);
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

    @Test
    // 创建一级目录，返回true
    public void testMkdirNormal001() throws Exception {
        Path testDir = new Path("/test_dir");
        fs.delete(testDir, true);

        boolean res = fs.mkdirs(testDir, new FsPermission((short) 00644));
        fs.delete(testDir, true);
    }

    @Test
    // 创建多级目录，父目录存在，返回true
    public void testMkdirNormal002() throws Exception {
        Path testDir = getTestPath("test_dir");

        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        fs.delete(testDir, true);
    }

    @Test
    // 创建多级目录，父目录及上级目录不存在，返回true
    public void testMkdirNormal003() throws Exception {
        Path testDir = getTestPath("a001/b001/test_dir");

        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        assertTrue(fs.exists(testDir.getParent()));
        assertTrue(fs.exists(testDir.getParent().getParent()));
        fs.delete(testDir, true);

        assertTrue(fs.mkdirs(testDir.getParent().getParent(),
            new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        fs.delete(testDir.getParent().getParent(), true);
    }

    @Test
    // 创建多级目录，父目录及上级目录不存在，创建各级父目录，并携带permission，返回true
    public void testMkdirNormal004() throws Exception {
        Path testDir = getTestPath("a001/b001/test_dir");

        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        assertTrue(fs.exists(testDir.getParent()));
        assertEquals(fs.getFileStatus(testDir).getPermission(),
            fs.getFileStatus(testDir.getParent()).getPermission());
        assertTrue(fs.exists(testDir.getParent().getParent()));
        assertEquals(fs.getFileStatus(testDir).getPermission(),
            fs.getFileStatus(testDir.getParent().getParent()).getPermission());
        fs.delete(testDir, true);
    }

    @Test
    // 创建多级目录，父目录及上级目录不存在，返回true
    public void testMkdirNormal005() throws Exception {
        Path testDir = getTestPath("a001/b001/test_dir");

        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        fs.delete(testDir, true);

        assertTrue(fs.mkdirs(testDir.getParent().getParent(),
            new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        fs.delete(testDir.getParent().getParent(), true);
    }

    @Test
    // 路径存在并且是一个目录，返回true
    public void testMkdirNormal006() throws Exception {
        Path testDir = getTestPath("a001/b001/test_dir");

        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));

        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        fs.delete(testDir.getParent().getParent(), true);
    }

    @Test
    // 路径已存在同名文件，抛FileAlreadyExistException
    public void testMkdirAbnormal001() throws Exception {
        Path testDir = getTestPath("test_dir");

        FSDataOutputStream stream = fs.create(testDir, false);
        byte[] data = ContractTestUtils.dataset(16, 0, 16);
        stream.write(data);
        stream.close();
        assertTrue(fs.getFileStatus(testDir).isFile());

        boolean hasException = false;
        try {
            fs.mkdirs(testDir, new FsPermission((short) 00644));
        } catch (FileAlreadyExistsException e) {
            hasException = true;
        }
        assertTrue(hasException);
        fs.delete(testDir, true);
    }

    @Test
    // 路径的父目录及上级目录不是一个目录，抛ParentNotDirectoryException
    public void testMkdirAbnormal002() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testDir = getTestPath("a001/b001/test_dir");

        FSDataOutputStream stream = fs.create(testDir.getParent(), false);
        byte[] data = ContractTestUtils.dataset(16, 0, 16);
        stream.write(data);
        stream.close();
        assertTrue(fs.getFileStatus(testDir.getParent()).isFile());

        boolean hasException = false;
        try {
            fs.mkdirs(testDir, new FsPermission((short) 00644));
        } catch (ParentNotDirectoryException e) {
            hasException = true;
        }
        assertTrue(hasException);
        fs.delete(testDir.getParent().getParent(), true);

        stream = fs.create(testDir.getParent().getParent(), false);
        stream.write(data);
        stream.close();

        hasException = false;
        try {
            fs.mkdirs(testDir, new FsPermission((short) 00644));
        } catch (ParentNotDirectoryException e) {
            hasException = true;
        }
        assertTrue(hasException);
        fs.delete(testDir.getParent().getParent(), true);
    }
}

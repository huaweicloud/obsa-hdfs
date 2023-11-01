package org.apache.hadoop.fs.obs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

import java.lang.reflect.Field;
import java.util.concurrent.ThreadPoolExecutor;

public class ITestOBSCloseFunction {
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
        fs = OBSTestUtils.createTestFileSystem(conf);
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
    // close时应关闭所有的OutputStream
    public void testOutputStreamClose() throws Exception {
        Path testFile = getTestPath("test_file");
        fs.delete(testFile);
        FSDataOutputStream outputStream = fs.create(testFile, false);
        OBSBlockOutputStream obsBlockOutputStream
            = (OBSBlockOutputStream) outputStream.getWrappedStream();
        Field field = obsBlockOutputStream.getClass()
            .getDeclaredField("closed");
        field.setAccessible(true);
        boolean closed = (boolean) field.get(obsBlockOutputStream);
        assertFalse(closed);
        fs.close();
        closed = (boolean) field.get(obsBlockOutputStream);
        fs = null;
        assertTrue(closed);
    }

    @Test
    // finalize接口释放实例申请的各类资源
    public void testThreadPoolClose() throws Throwable {

        Field boundedCopyThreadPoolField = fs.getClass()
            .getDeclaredField("boundedCopyThreadPool");
        boundedCopyThreadPoolField.setAccessible(true);
        ThreadPoolExecutor boundedCopyThreadPool
            = (ThreadPoolExecutor) boundedCopyThreadPoolField.get(fs);

        Field boundedDeleteThreadPoolField = fs.getClass()
            .getDeclaredField("boundedDeleteThreadPool");
        boundedDeleteThreadPoolField.setAccessible(true);
        ThreadPoolExecutor boundedDeleteThreadPool
            = (ThreadPoolExecutor) boundedDeleteThreadPoolField.get(fs);

        Field boundedCopyPartThreadPoolField = fs.getClass()
            .getDeclaredField("boundedCopyPartThreadPool");
        boundedCopyPartThreadPoolField.setAccessible(true);
        ThreadPoolExecutor boundedCopyPartThreadPool
            = (ThreadPoolExecutor) boundedCopyPartThreadPoolField.get(fs);

        Field boundedListThreadPoolField = fs.getClass()
            .getDeclaredField("boundedListThreadPool");
        boundedListThreadPoolField.setAccessible(true);
        ThreadPoolExecutor boundedListThreadPool
            = (ThreadPoolExecutor) boundedListThreadPoolField.get(fs);

        fs.close();
        fs = null;

        if (boundedCopyThreadPool != null) {
            assertTrue(boundedCopyThreadPool.isShutdown());
        }
        if (boundedDeleteThreadPool != null) {
            assertTrue(boundedDeleteThreadPool.isShutdown());
        }
        if (boundedCopyPartThreadPool != null) {
            assertTrue(boundedCopyPartThreadPool.isShutdown());
        }
        if (boundedListThreadPool != null) {
            assertTrue(boundedListThreadPool.isShutdown());
        }
    }
}

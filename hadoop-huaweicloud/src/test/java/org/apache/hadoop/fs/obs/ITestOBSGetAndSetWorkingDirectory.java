package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public class ITestOBSGetAndSetWorkingDirectory {
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
            fs.delete(new Path(testRootPath), true);
        }
    }

    @Test
    // 设置WorkingDirectory时相对路径转化为绝对路径赋值给当前工作目录workingDir
    public void testWorkingDirectory01() throws Exception {
        String path = "a001/b001";
        Path workPath = new Path(path);
        fs.setWorkingDirectory(workPath);
        assertTrue(fs.getWorkingDirectory()
            .toString()
            .startsWith(fs.getUri().toString()));
    }

    @Test
    // 设置WorkingDirectory时绝对路径赋值给当前工作目录workingDir
    public void testWorkingDirectory02() throws Exception {
        String path = fs.getUri().toString() + "/" + "a001/b001";
        Path workPath = new Path(path);
        fs.setWorkingDirectory(workPath);
        assertEquals(workPath, fs.getWorkingDirectory());
    }

    @Test
    // 设置WorkingDirectory时，需检查路径有效性，不包含"."、":"、"//"等不合法的字符
    public void testWorkingDirectory03() throws Exception {
        // 实际不满足，需整改
        Path workPath = new Path("a001/./b001");
        boolean hasException = false;
        try {
            fs.setWorkingDirectory(workPath);
        } catch (IllegalArgumentException e) {
            hasException = true;
        }
        assertFalse(hasException);

        try {
            workPath = new Path("a:a001");
            hasException = false;
            fs.setWorkingDirectory(workPath);
        } catch (IllegalArgumentException e) {
            hasException = true;
        }
        assertTrue(hasException);

        workPath = new Path("//a001");
        hasException = false;
        try {
            fs.setWorkingDirectory(workPath);
        } catch (IllegalArgumentException e) {
            hasException = true;
        }
        assertTrue(hasException);
    }
}

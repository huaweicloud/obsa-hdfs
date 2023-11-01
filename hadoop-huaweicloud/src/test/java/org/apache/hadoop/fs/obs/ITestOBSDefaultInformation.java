package org.apache.hadoop.fs.obs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

public class ITestOBSDefaultInformation {
    private OBSFileSystem fs;
    private Configuration conf;
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
        conf = OBSContract.getConfiguration(null);
        fs = OBSTestUtils.createTestFileSystem(conf);
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            OBSFSTestUtil.deletePathRecursive(fs, new Path(testRootPath));
        }
    }

    @Test
    // 返回客户端可配的默认的BlockSize
    public void testGetDefaultBlockSize01() throws Exception {
        long defaultBlockSize = 64 * 1024 * 1024;
        conf.setLong(OBSConstants.FS_OBS_BLOCK_SIZE, defaultBlockSize);

        OBSFileSystem obsFs = OBSTestUtils.createTestFileSystem(conf);
        assertEquals(defaultBlockSize, obsFs.getDefaultBlockSize());
    }

    @Test
    // 校验BlockSize默认值为128MB
    public void testGetDefaultBlockSize02() {
        assertEquals(128 * 1024 * 1024, fs.getDefaultBlockSize());
    }

    @Test
    // BlockSize由fs.obs.block.size配置决定，修改dfs.blocksize应无效
    public void testGetDefaultBlockSize03() throws Exception {
        conf.setLong(OBSConstants.FS_OBS_BLOCK_SIZE, 128 * 1024 * 1024);
        conf.setLong("dfs.blocksize", 64 * 1024 * 1024);

        OBSFileSystem obsFs = OBSTestUtils.createTestFileSystem(conf);
        assertEquals(128 * 1024 * 1024, obsFs.getDefaultBlockSize());
    }

    @Test
    // getHomeDirectory()要返回用户的Home路径："/user/" + ugi.getShortUserName()
    public void testGetHomeDirectory() {
        String homeDirectory = fs.getHomeDirectory().toString();

        String username;
        try {
            username = UserGroupInformation.getCurrentUser().getShortUserName();
        } catch (IOException ex) {
            username = System.getProperty("user.name");
        }

        assertTrue(homeDirectory.endsWith("/user/" + username));
    }
}

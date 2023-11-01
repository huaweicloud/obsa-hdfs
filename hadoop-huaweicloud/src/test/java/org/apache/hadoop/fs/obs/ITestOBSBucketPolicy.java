package org.apache.hadoop.fs.obs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.fs.permission.FsPermission;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ITestOBSBucketPolicy {
    /**
     * Resources_s3 前缀
     */
    private static final String RES_S3_PREFIX = "arn:aws:s3:::";

    /**
     * Resources_IAM 前缀
     */
    private static final String RES_IAM_PREFIX = "arn:aws:iam::";

    private static final String NEW_USER_AK = "UDSIAMSTUBTEST000999";

    private static final String NEW_USER_SK
        = "Udsiamstubtest000000UDSIAMSTUBTEST000999";

    private static final String NEW_USER_DOMAIN_ID
        = "domainiddomainiddomainiddo000999";

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
        fs.delete(new Path(testRootPath), true);
        fs.mkdirs(new Path(testRootPath), new FsPermission((short) 00644));
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

    private JSONObject getOtherTenantAccessPolicy(Path testDir) throws JSONException {
        return new JSONObject().put("Statement", new JSONArray().put(new JSONObject().put("Effect", "Allow")
            .put("Sid", "1")
            .put("Principal",
                new JSONObject().put("AWS", new JSONArray().put(RES_IAM_PREFIX + NEW_USER_DOMAIN_ID + ":root")))
            .put("Action", new JSONArray().put("*"))
            .put("Resource", new JSONArray()
                .put(RES_S3_PREFIX + fs.getBucket())
                .put(RES_S3_PREFIX + fs.getBucket() + testRootPath + "/")
                .put(RES_S3_PREFIX + fs.getBucket() + "/" + OBSCommonUtils.pathToKey(fs, testDir) + "/*")
                .put(RES_S3_PREFIX + fs.getBucket() + "/" + OBSCommonUtils.pathToKey(fs, testDir))
            )));
    }

    @Test
    // 配置policy后，创建目录权限校验
    public void testMkdir() throws IOException, JSONException, InterruptedException {
        if(!fs.isFsBucket()) {
            return;
        }

        Path testDir = getTestPath("test_dir");
        fs.delete(testDir, true);

        setPolicy(getOtherTenantAccessPolicy(testDir));

        Configuration conf = OBSContract.getConfiguration(null);
        conf.set(OBSConstants.ACCESS_KEY, NEW_USER_AK);
        conf.set(OBSConstants.SECRET_KEY, NEW_USER_SK);
        OBSFileSystem newFs = OBSTestUtils.createTestFileSystem(conf);

        assertTrue(newFs.mkdirs(testDir, new FsPermission((short) 00644)));

        newFs.delete(testDir, true);
    }

    private void setPolicy(JSONObject testDir) throws InterruptedException {
        fs.setBucketPolicy(testDir.toString());
        // policy 有缓存，需要3秒才能生效
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    // 配置policy后，listStatus权限校验
    public void testListStatus() throws IOException, JSONException, InterruptedException {
        if(!fs.isFsBucket()) {
            return;
        }

        Path testDir = getTestPath("test_dir");
        fs.delete(testDir, true);

        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1"),
            new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1/subsubdir1"),
            new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir2"),
            new FsPermission((short) 00644)));

        setPolicy(getOtherTenantAccessPolicy(testDir));

        Configuration conf = OBSContract.getConfiguration(null);
        conf.set(OBSConstants.ACCESS_KEY, NEW_USER_AK);
        conf.set(OBSConstants.SECRET_KEY, NEW_USER_SK);
        OBSFileSystem newFs = OBSTestUtils.createTestFileSystem(conf);

        assertEquals(2, newFs.listStatus(testDir).length);
        assertEquals(3, newFs.listStatus(testDir, true).length);

        fs.delete(testDir, true);
    }

    @Test
    // 配置policy后，listFiles权限校验
    public void testListFiles() throws IOException, JSONException, InterruptedException {
        if(!fs.isFsBucket()) {
            return;
        }

        Path testDir = getTestPath("test_dir");
        fs.delete(testDir, true);

        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1"),
            new FsPermission((short) 00644)));
        FSDataOutputStream outputStream = fs.create(getTestPath("test_dir"
            + "/subdir1/file"), false);
        outputStream.close();
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1/subsubdir1"),
            new FsPermission((short) 00644)));
        outputStream = fs.create(getTestPath("test_dir"
            + "/subdir1/subsubdir1/file"), false);
        outputStream.close();
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir2"),
            new FsPermission((short) 00644)));
        outputStream = fs.create(getTestPath("test_dir"
            + "/subdir2/file"), false);
        outputStream.close();

        setPolicy(getOtherTenantAccessPolicy(testDir));

        Configuration conf = OBSContract.getConfiguration(null);
        conf.set(OBSConstants.ACCESS_KEY, NEW_USER_AK);
        conf.set(OBSConstants.SECRET_KEY, NEW_USER_SK);
        OBSFileSystem newFs = OBSTestUtils.createTestFileSystem(conf);

        RemoteIterator<LocatedFileStatus> iterator = newFs.listFiles(testDir,
            true);
        int fileNum = 0;
        while (iterator.hasNext()) {
            iterator.next();
            fileNum++;
        }
        assertEquals(3, fileNum);

        fs.delete(testDir, true);
    }

    @Test
    // 配置policy后，ListLocatedStatus权限校验
    public void testListLocatedStatus() throws IOException, JSONException, InterruptedException {
        if(!fs.isFsBucket()) {
            return;
        }

        Path testDir = getTestPath("test_dir");
        fs.delete(testDir, true);

        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1"),
            new FsPermission((short) 00644)));
        FSDataOutputStream outputStream = fs.create(getTestPath("test_dir"
            + "/subdir1/file"), false);
        outputStream.close();
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1/subsubdir1"),
            new FsPermission((short) 00644)));
        outputStream = fs.create(getTestPath("test_dir"
            + "/subdir1/subsubdir1/file"), false);
        outputStream.close();
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir2"),
            new FsPermission((short) 00644)));
        outputStream = fs.create(getTestPath("test_dir"
            + "/subdir2/file"), false);
        outputStream.close();

        setPolicy(getOtherTenantAccessPolicy(testDir));

        Configuration conf = OBSContract.getConfiguration(null);
        conf.set(OBSConstants.ACCESS_KEY, NEW_USER_AK);
        conf.set(OBSConstants.SECRET_KEY, NEW_USER_SK);
        OBSFileSystem newFs = OBSTestUtils.createTestFileSystem(conf);

        RemoteIterator<LocatedFileStatus> iterator = newFs.listLocatedStatus(
            testDir);
        int fileNum = 0;
        while (iterator.hasNext()) {
            iterator.next();
            fileNum++;
        }
        assertEquals(2, fileNum);

        fs.delete(testDir, true);
    }

    @Test
    // 配置policy后，GetContentSummary权限校验
    public void testGetContentSummary() throws IOException, JSONException, InterruptedException {
        if(!fs.isFsBucket()) {
            return;
        }

        Path testDir = getTestPath("test_dir");
        fs.delete(testDir, true);

        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1"),
            new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1/subsubdir1"),
            new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir2"),
            new FsPermission((short) 00644)));

        setPolicy(getOtherTenantAccessPolicy(testDir));

        Configuration conf = OBSContract.getConfiguration(null);
        conf.set(OBSConstants.ACCESS_KEY, NEW_USER_AK);
        conf.set(OBSConstants.SECRET_KEY, NEW_USER_SK);
        OBSFileSystem newFs = OBSTestUtils.createTestFileSystem(conf);

        assertEquals(4, newFs.getContentSummary(testDir).getDirectoryCount());

        fs.delete(testDir, true);
    }

    @Test
    // 配置policy后，GetFileStatus权限校验
    public void testGetFileStatus() throws IOException, JSONException, InterruptedException {
        if(!fs.isFsBucket()) {
            return;
        }

        Path testDir = getTestPath("test_dir");
        fs.delete(testDir, true);

        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1"),
            new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1/subsubdir1"),
            new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir2"),
            new FsPermission((short) 00644)));

        setPolicy(getOtherTenantAccessPolicy(testDir));

        Configuration conf = OBSContract.getConfiguration(null);
        conf.set(OBSConstants.ACCESS_KEY, NEW_USER_AK);
        conf.set(OBSConstants.SECRET_KEY, NEW_USER_SK);
        OBSFileSystem newFs = OBSTestUtils.createTestFileSystem(conf);

        assertTrue(newFs.getFileStatus(testDir).isDirectory());

        fs.delete(testDir, true);
    }

    @Test
    // 配置policy后，Exist权限校验
    public void testExist() throws IOException, JSONException, InterruptedException {
        if(!fs.isFsBucket()) {
            return;
        }

        Path testDir = getTestPath("test_dir");
        fs.delete(testDir, true);

        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1"),
            new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1/subsubdir1"),
            new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir2"),
            new FsPermission((short) 00644)));

        setPolicy(getOtherTenantAccessPolicy(testDir));

        Configuration conf = OBSContract.getConfiguration(null);
        conf.set(OBSConstants.ACCESS_KEY, NEW_USER_AK);
        conf.set(OBSConstants.SECRET_KEY, NEW_USER_SK);
        OBSFileSystem newFs = OBSTestUtils.createTestFileSystem(conf);

        assertTrue(newFs.exists(testDir));

        fs.delete(testDir, true);
    }

    @Test
    // 配置policy后，Delete权限校验
    public void testDelete() throws IOException, JSONException, InterruptedException {
        if(!fs.isFsBucket()) {
            return;
        }

        Path testDir = getTestPath("test_dir");
        fs.delete(testDir, true);

        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1"),
            new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1/subsubdir1"),
            new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir2"),
            new FsPermission((short) 00644)));

        setPolicy(getOtherTenantAccessPolicy(testDir));

        Configuration conf = OBSContract.getConfiguration(null);
        conf.set(OBSConstants.ACCESS_KEY, NEW_USER_AK);
        conf.set(OBSConstants.SECRET_KEY, NEW_USER_SK);
        OBSFileSystem newFs = OBSTestUtils.createTestFileSystem(conf);

        assertTrue(newFs.delete(testDir, true));
    }

    @Test
    // 配置policy后，Rename权限校验
    public void testRename() throws IOException, JSONException, InterruptedException {
        if(!fs.isFsBucket()) {
            return;
        }

        Path testDir = getTestPath("test_dir");
        Path renameToDir = getTestPath("test_dir_to");
        fs.delete(testDir, true);

        assertTrue(fs.mkdirs(testDir, new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1"),
            new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir1/subsubdir1"),
            new FsPermission((short) 00644)));
        assertTrue(fs.mkdirs(getTestPath("test_dir/subdir2"),
            new FsPermission((short) 00644)));

        setPolicy(new JSONObject().put("Statement", new JSONArray().put(new JSONObject().put("Effect", "Allow")
            .put("Sid", "1")
            .put("Principal",
                new JSONObject().put("AWS", new JSONArray().put(RES_IAM_PREFIX + NEW_USER_DOMAIN_ID + ":root")))
            .put("Action", new JSONArray().put("*"))
            .put("Resource", new JSONArray().put(RES_S3_PREFIX + fs.getBucket())
                .put(RES_S3_PREFIX + fs.getBucket() + testRootPath + "/")
                .put(RES_S3_PREFIX + fs.getBucket() + "/" + OBSCommonUtils.pathToKey(fs, testDir) + "/*")
                .put(RES_S3_PREFIX + fs.getBucket() + "/" + OBSCommonUtils.pathToKey(fs, testDir))
                .put(RES_S3_PREFIX + fs.getBucket() + "/" + OBSCommonUtils.pathToKey(fs, renameToDir) + "/*")
                .put(RES_S3_PREFIX + fs.getBucket() + "/" + OBSCommonUtils.pathToKey(fs, renameToDir))))));

        Configuration conf = OBSContract.getConfiguration(null);
        conf.set(OBSConstants.ACCESS_KEY, NEW_USER_AK);
        conf.set(OBSConstants.SECRET_KEY, NEW_USER_SK);
        OBSFileSystem newFs = OBSTestUtils.createTestFileSystem(conf);

        assertTrue(newFs.rename(testDir, renameToDir));

        fs.delete(renameToDir, true);
    }
}

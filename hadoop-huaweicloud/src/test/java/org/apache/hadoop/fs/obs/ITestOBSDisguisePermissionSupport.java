package org.apache.hadoop.fs.obs;

import com.obs.services.model.SetObjectMetadataRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ITestOBSDisguisePermissionSupport {
    private OBSFileSystem fs;

    private static String testRootPath = OBSTestUtils.generateUniqueTestPath();

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        conf.set(OBSConstants.PERMISSIONS_MODE, OBSConstants.PERMISSIONS_MODE_DISGUISE);
        fs = OBSTestUtils.createTestFileSystem(conf);
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }

    private Path getTestPath(String relativePath) {
        return new Path(testRootPath, relativePath);
    }

    @Test
    public void testOBSSetOwner() throws Exception {
        if (!fs.supportDisguisePermissionsMode()) {
            return;
        }
        Path testFile = getTestPath("file_for_permission_test");
        FSDataOutputStream outputStream = fs.create(testFile, false);
        outputStream.close();
        fs.setOwner(testFile, "hadoop", "root");
        Map<String, Object> objMap = fs.getObsClient().getObjectMetadata(fs.getBucket(),
            OBSCommonUtils.pathToKey(fs, testFile)).getAllMetadata();
        assertEquals("hadoop", objMap.get("user").toString());
        assertEquals("root", objMap.get("group").toString());
    }

    @Test
    public void testOBSSetPermission() throws Exception {
        if (!fs.supportDisguisePermissionsMode()) {
            return;
        }
        Path testFile = getTestPath("file_for_owner_and_group_test");
        FSDataOutputStream outputStream = fs.create(testFile, false);
        outputStream.close();
        fs.setPermission(testFile, new FsPermission((short)00400));
        Map<String, Object> objMap = fs.getObsClient().getObjectMetadata(fs.getBucket(),
            OBSCommonUtils.pathToKey(fs, testFile)).getAllMetadata();
        assertEquals("256", objMap.get("permission").toString());
    }

    @Test
    public void testOBSGetFileStatusForFile() throws Exception {
        if (!fs.supportDisguisePermissionsMode()) {
            return;
        }
        Path testFile= getTestPath("file_for_get_file_status_test");
        FSDataOutputStream outputStream = fs.create(testFile);
        outputStream.close();
        testOBDGetFileStatus(testFile);
    }

    @Test
    public void testOBSGetFileStatusForFolder() throws Exception {
        if (!fs.supportDisguisePermissionsMode()) {
            return;
        }
        Path testFolder = getTestPath("folder_for_get_file_status_test");
        fs.mkdirs(testFolder);
        testOBDGetFileStatus(testFolder);
    }

    private void testOBDGetFileStatus(Path p) throws Exception {
        fs.setPermission(p, new FsPermission((short) 00640));
        fs.setOwner(p, "root", "supergroup");
        FileStatus status = fs.getFileStatus(p);
        assertEquals("rw-r-----", status.getPermission().toString());
        assertEquals("root", status.getOwner());
        assertEquals("supergroup", status.getGroup());
    }

    @Test
    public void testOBSGetFileStatusPermissionInvalid() throws Exception {
        if (!fs.supportDisguisePermissionsMode()) {
            return;
        }
        Path testFile= getTestPath("file_for_get_file_status_test");
        FSDataOutputStream outputStream = fs.create(testFile);
        outputStream.close();
        SetObjectMetadataRequest req = new SetObjectMetadataRequest(fs.getBucket(),
            OBSCommonUtils.pathToKey(fs, testFile));
        req.addUserMetadata("permission", "invalid_permission");
        fs.getObsClient().setObjectMetadata(req);
        FileStatus status = fs.getFileStatus(testFile);
        assertEquals("rw-rw-rw-", status.getPermission().toString());
    }

    @Test
    public void testOBSListFiles() throws Exception {
        if (!fs.supportDisguisePermissionsMode()) {
            return;
        }
        Path testFolder = getTestPath("folder_for_list_files_test");
        fs.mkdirs(testFolder);
        fs.setPermission(testFolder, new FsPermission((short) 00644));
        fs.setOwner(testFolder, "root", "supergroup");

        Path testFile = getTestPath("file_for_list_files_test");
        FSDataOutputStream outputStream = fs.create(testFile);
        outputStream.close();
        fs.setPermission(testFile, new FsPermission((short) 00644));
        fs.setOwner(testFile, "root", "supergroup");

        RemoteIterator<LocatedFileStatus> fileStatus = fs.listFiles(new Path(testRootPath), true);
        while(fileStatus.hasNext()) {
            LocatedFileStatus status = fileStatus.next();
            assertEquals("rw-r--r--", status.getPermission().toString());
            assertEquals("root", status.getOwner());
            assertEquals("supergroup", status.getGroup());
        }
    }
}

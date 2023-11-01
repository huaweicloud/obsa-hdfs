package org.apache.hadoop.fs.obs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import com.obs.services.model.fs.FSStatusEnum;
import com.obs.services.model.fs.GetBucketFSStatusResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.security.AccessControlException;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;

@RunWith(MockitoJUnitRunner.class)
public class ITestOBSRetryMechanism {
    private OBSFileSystem fs;

    private OBSFileSystem mockFs;

    private ObsClient obsClient;

    private ObsClient mockObsClient;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private Path testFile = getTestPath("testFile");

    private Path testDir = getTestPath("testDir");

    private static byte[] dataSet = ContractTestUtils.dataset(16, 0, 10);

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        conf.setBoolean(OBSConstants.FAST_UPLOAD, false);
        conf.setLong(OBSConstants.RETRY_SLEEP_BASETIME, 2);

        fs = OBSTestUtils.createTestFileSystem(conf);
        obsClient = fs.getObsClient();
        initTestEnv();

        initMock();
    }

    private Path getTestPath(String testPath) {
        return new Path(testRootPath + "/" + testPath);
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
            fs.close();
        }
        Mockito.reset(mockFs, mockObsClient);
    }

    private void initTestEnv() throws Exception {
        FSDataOutputStream outputStream = fs.create(testFile, true);
        outputStream.write(dataSet);
        outputStream.close();
        assertTrue(fs.mkdirs(testDir));
        if (!fs.exists(testDir)) {
            throw new IOException("testDir not exist!");
        }
    }

    private void initMock() {
        mockFs = Mockito.spy(fs);
        mockObsClient = Mockito.spy(obsClient);
        Whitebox.setInternalState(mockFs, obsClient, mockObsClient);
    }

    @Test
    // 测试GetFileStatus重试机制，重试时对于403等特殊异常能正确识别
    public void testGetFileStatus() throws IOException {
        OBSFileConflictException fileConflictException = new OBSFileConflictException(
            "mock FileConflictException");
        FileNotFoundException fileNotFoundException = new FileNotFoundException(
            "mock FileNotFoundException");
        AccessControlException accessControlException =
            new AccessControlException("mock AccessControlException");
        OBSIOException obsioException = new OBSIOException("mock IOException",
            new ObsException("mock ObsException"));

        Mockito.doThrow(obsioException)
            .doThrow(obsioException)
            .doThrow(obsioException)
            .doThrow(obsioException)
            .doThrow(obsioException)
            .doThrow(fileNotFoundException)
            .doThrow(obsioException)
            .doThrow(obsioException)
            .doThrow(fileConflictException)
            .doThrow(obsioException)
            .doThrow(obsioException)
            .doThrow(accessControlException)
            .doThrow(obsioException)
            .doThrow(obsioException)
            .doThrow(obsioException)
            .doCallRealMethod()
            .when(mockFs)
            .innerGetFileStatus(anyObject());

        int i = 0;
        while (true) {
            try {
                mockFs.getFileStatus(testFile);
                break;
            } catch (FileNotFoundException e) {
                assertTrue(i == 0 || i == 1);
            } catch (AccessControlException e) {
                assertTrue(i == 2);
            }
            i++;
        }
        assertTrue(i == 3);
    }

    @Test
    // 测试IsFolderEmpty重试机制，重试时对于403等特殊异常能正确识别
    public void testIsFolderEmpty() throws Exception {
        ObsException obsException = new ObsException("mock ObsException.");
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);
        ObsException conflictException = new ObsException("mock "
            + "conflict exception.");
        conflictException.setResponseCode(OBSCommonUtils.CONFLICT_CODE);

        Mockito.doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(notFoundException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(conflictException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doCallRealMethod()
            .when(mockObsClient)
            .listObjects((ListObjectsRequest) anyObject());

        int i = 0;
        while (true) {
            try {
                OBSCommonUtils.isFolderEmpty(mockFs,
                    testDir.toString().substring(1));
                break;
            } catch (AccessControlException e) {
                assertTrue(i == 0 || i == 1);
            } catch (FileNotFoundException e) {
                assertTrue(i == 2);
            } catch (OBSFileConflictException e) {
                assertTrue(i == 3);
            }
            i++;
        }
        assertTrue(i == 4);
    }

    @Test
    // 测试CreateEmptyObject重试机制，重试时对于403等特殊异常能正确识别
    public void testCreateEmptyObject() throws Exception {
        if (fs.isFsBucket()) {
            return;
        }

        ObsException obsException = new ObsException("mock ObsException.");
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);
        ObsException conflictException = new ObsException("mock "
            + "conflict exception.");
        conflictException.setResponseCode(OBSCommonUtils.CONFLICT_CODE);

        Mockito.doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(notFoundException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(conflictException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doCallRealMethod()
            .doCallRealMethod()
            .when(mockObsClient)
            .putObject(anyObject());

        int i = 0;
        while (true) {
            try {
                OBSObjectBucketUtils.createEmptyObject(mockFs,
                    OBSCommonUtils.pathToKey(fs, getTestPath("testEmptyDir")));
                break;
            } catch (AccessControlException e) {
                assertTrue(i == 0 || i == 1);
            } catch (FileNotFoundException e) {
                assertTrue(i == 2);
            } catch (OBSFileConflictException e) {
                assertTrue(i == 3);
            }
            i++;
        }
        assertTrue(i == 4);
    }

    @Test
    // 测试FsCreateFolder重试机制，重试时对于403等特殊异常能正确识别
    public void testFsCreateFolder() throws IOException {
        ObsException obsException = new ObsException("mock ObsException.");
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);
        ObsException conflictException = new ObsException("mock "
            + "conflict exception.");
        conflictException.setResponseCode(OBSCommonUtils.CONFLICT_CODE);

        Mockito.doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(notFoundException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(conflictException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doCallRealMethod()
            .when(mockObsClient)
            .newFolder(anyObject());

        int i = 0;
        while (true) {
            try {
                OBSPosixBucketUtils.fsCreateFolder(mockFs,
                    OBSCommonUtils.pathToKey(fs, getTestPath("testFsCreateFolder")));
                break;
            } catch (AccessControlException e) {
                assertTrue(i == 0 || i == 1);
            } catch (FileNotFoundException e) {
                assertTrue(i == 2);
            } catch (OBSFileConflictException e) {
                assertTrue(i == 3);
            }
            i++;
        }
        assertTrue(i == 4);
    }

    @Test
    // 测试ListObjects重试机制，重试时对于403等特殊异常能正确识别
    public void testListObjects() throws Exception {
        ObsException obsException = new ObsException("mock ObsException.");
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);
        ObsException conflictException = new ObsException("mock "
            + "conflict exception.");
        conflictException.setResponseCode(OBSCommonUtils.CONFLICT_CODE);

        Mockito.doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(notFoundException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(conflictException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doCallRealMethod()
            .when(mockObsClient)
            .listObjects((ListObjectsRequest) anyObject());

        int i = 0;
        while (true) {
            try {
                ListObjectsRequest request = new ListObjectsRequest();
                request.setBucketName(mockFs.getBucket());
                request.setPrefix(testRootPath.substring(1));
                request.setDelimiter("/");
                request.setMaxKeys(1000);

                OBSCommonUtils.listObjects(mockFs, request);
                break;
            } catch (AccessControlException e) {
                assertTrue(i == 0 || i == 1);
            } catch (FileNotFoundException e) {
                assertTrue(i == 2);
            } catch (OBSFileConflictException e) {
                assertTrue(i == 3);
            }
            i++;
        }
        assertTrue(i == 4);
    }

    @Test
    // 测试ListObjectsRecursively重试机制，重试时对于403等特殊异常能正确识别
    public void testListObjectsRecursively() throws Exception {
        ObsException obsException = new ObsException("mock ObsException.");
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);
        ObsException conflictException = new ObsException("mock "
            + "conflict exception.");
        conflictException.setResponseCode(OBSCommonUtils.CONFLICT_CODE);

        Mockito.doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(notFoundException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(conflictException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doCallRealMethod()
            .when(mockObsClient)
            .listObjects((ListObjectsRequest) anyObject());

        int i = 0;
        while (true) {
            try {
                ListObjectsRequest request = new ListObjectsRequest();
                request.setBucketName(mockFs.getBucket());
                request.setPrefix(testRootPath.substring(1));
                request.setMaxKeys(1000);

                OBSCommonUtils.listObjects(mockFs, request);
                break;
            } catch (AccessControlException e) {
                assertTrue(i == 0 || i == 1);
            } catch (FileNotFoundException e) {
                assertTrue(i == 2);
            } catch (OBSFileConflictException e) {
                assertTrue(i == 3);
            }
            i++;
        }
        assertTrue(i == 4);
    }

    @Test
    // 测试ContinueListObjects重试机制，重试时对于403等特殊异常能正确识别
    public void testContinueListObjects() throws Exception {
        ObsException obsException = new ObsException("mock ObsException.");
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);
        ObsException conflictException = new ObsException("mock "
            + "conflict exception.");
        conflictException.setResponseCode(OBSCommonUtils.CONFLICT_CODE);

        Mockito.doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(notFoundException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(conflictException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doCallRealMethod()
            .when(mockObsClient)
            .listObjects((ListObjectsRequest) anyObject());

        int i = 0;
        while (true) {
            try {
                ListObjectsRequest request = new ListObjectsRequest();
                request.setBucketName(mockFs.getBucket());
                request.setPrefix(testRootPath.substring(1));
                request.setDelimiter("/");
                request.setMaxKeys(1000);

                ObjectListing objectListing = new ObjectListing(null, null,
                    mockFs.getBucket(), true, testRootPath.substring(1), null,
                    1000, "/", "/", null);

                OBSCommonUtils.continueListObjects(mockFs, objectListing);
                break;
            } catch (AccessControlException e) {
                assertTrue(i == 0 || i == 1);
            } catch (FileNotFoundException e) {
                assertTrue(i == 2);
            } catch (OBSFileConflictException e) {
                assertTrue(i == 3);
            }
            i++;
        }
        assertTrue(i == 4);
    }

    @Test
    // 与testListObjectsRecursively的测试点重复，不用再重复测试
    public void testContinueListObjectsRecursively() {
    }

    @Test
    // 测试InnerFsTruncateWithRetry重试机制，重试时对于403等特殊异常能正确识别
    public void testInnerFsTruncateWithRetry() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }

        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile, false);
        byte[] data = {0, 0, 0, 0, 0, 0};
        outputStream.write(data);
        outputStream.close();

        ObsException obsException = new ObsException("mock ObsException.");
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);
        ObsException conflictException = new ObsException("mock "
            + "conflict exception.");
        conflictException.setResponseCode(OBSCommonUtils.CONFLICT_CODE);

        Mockito.doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(notFoundException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(conflictException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doCallRealMethod()
            .when(mockObsClient)
            .truncateObject(anyObject());

        int i = 0;
        while (true) {
            try {
                OBSPosixBucketUtils.innerFsTruncateWithRetry(mockFs, testFile, 3);
                break;
            } catch (AccessControlException e) {
                assertTrue(i == 0 || i == 1);
            } catch (FileNotFoundException e) {
                assertTrue(i == 2);
            } catch (OBSFileConflictException e) {
                assertTrue(i == 3);
            }
            i++;
        }
        assertTrue(i == 4);
    }

    @Test
    // 测试FileInnerFsRenameWithRetry重试机制，重试时对于403等特殊异常能正确识别
    public void testFileInnerFsRenameWithRetry() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }

        Path testFileSrc = getTestPath("test_file_src");
        FSDataOutputStream outputStream = fs.create(testFileSrc, false);
        byte[] data = {0, 0, 0, 0, 0, 0};
        outputStream.write(data);
        outputStream.close();
        Path testFileDst = getTestPath("test_file_dst");
        String srcKey = OBSCommonUtils.pathToKey(fs, testFileSrc);
        String dstKey = OBSCommonUtils.pathToKey(fs, testFileDst);

        ObsException obsException = new ObsException("mock ObsException.");
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);
        ObsException conflictException = new ObsException("mock "
            + "conflict exception.");
        conflictException.setResponseCode(OBSCommonUtils.CONFLICT_CODE);

        Mockito.doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doCallRealMethod()
            .doThrow(notFoundException)
            .when(mockObsClient)
            .renameFile(anyObject());

        int i = 0;
        boolean success;
        while (i < 4) {
            try {
                success = OBSPosixBucketUtils.innerFsRenameWithRetry(mockFs,
                    testFileSrc, testFileDst, srcKey, dstKey);
                assertTrue(success);
            } catch (AccessControlException e) {
                assertTrue(i == 0 || i == 1);
            }
            i++;
        }
    }

    @Test
    // 测试DirInnerFsRenameWithRetry重试机制，重试时对于403等特殊异常能正确识别
    public void testDirInnerFsRenameWithRetry() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }

        Path testDirSrc = getTestPath("test_dir_src");
        fs.mkdirs(testDirSrc);
        Path testDirDst = getTestPath("test_dir_dst");
        String srcKey = OBSCommonUtils.pathToKey(fs, testDirSrc);
        String dstKey = OBSCommonUtils.pathToKey(fs, testDirDst);

        ObsException obsException = new ObsException("mock ObsException.");
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);
        ObsException conflictException = new ObsException("mock "
            + "conflict exception.");
        conflictException.setResponseCode(OBSCommonUtils.CONFLICT_CODE);

        Mockito.doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doCallRealMethod()
            .doThrow(notFoundException)
            .when(mockObsClient)
            .renameFile(anyObject());

        int i = 0;
        boolean success;
        while (i < 4) {
            try {
                success = OBSPosixBucketUtils.innerFsRenameWithRetry(mockFs,
                    testDirSrc, testDirDst, srcKey, dstKey);
                assertTrue(success);
            } catch (AccessControlException e) {
                assertTrue(i == 0 || i == 1);
            }
            i++;
        }
    }

    @Test
    // http ResponseCode 400 not retry
    public void testNotRetry400() throws IOException {
        OBSIllegalArgumentException illegalArgumentException = new OBSIllegalArgumentException(
            "mock OBSIllegalArgumentException");
        OBSIOException obsioException = new OBSIOException("mock IOException",
            new ObsException("mock ObsException"));

        Mockito.doThrow(obsioException)
            .doThrow(illegalArgumentException)
            .doThrow(obsioException)
            .doThrow(obsioException)
            .doThrow(obsioException)
            .doCallRealMethod()
            .when(mockFs)
            .innerGetFileStatus(anyObject());
        //OBSIllegalArgumentException not retry
        try {
            mockFs.getFileStatus(testFile);
        } catch (OBSIllegalArgumentException e) {
            assertTrue(true);
        }
        //OBSIOException retry
        try {
            mockFs.getFileStatus(testFile);
        } catch (IOException e) {
            assertTrue(false);
        }
    }

    @Test
    // 测试getBucketFsStatus重试机制
    public void testGetBucketFsStatus() throws Exception {
        ObsException obsException = new ObsException("mock ObsException.");
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);
        ObsException conflictException = new ObsException("mock "
            + "conflict exception.");
        conflictException.setResponseCode(OBSCommonUtils.CONFLICT_CODE);

        Mockito.doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(notFoundException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(conflictException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doReturn(new GetBucketFSStatusResult(null, null, 0, null, null,
                null, null, null, FSStatusEnum.ENABLED))
            .when(mockObsClient)
            .getBucketFSStatus(anyObject());

        int i = 0;
        while (true) {
            try {
                OBSCommonUtils.getBucketFsStatus(mockObsClient, "obsfilesystem-bucket");
                break;
            } catch (AccessControlException e) {
                assertTrue(i == 0 || i == 1);
            } catch (FileNotFoundException e) {
                assertTrue(i == 2);
            } catch (OBSFileConflictException e) {
                assertTrue(i == 3);
            }
            i++;
        }
        assertTrue(i == 4);
    }

    @Test
    // 测试lazySeek onReadFailure时对reopen的重试机制
    public void testLazySeek() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile, false);
        byte[] data = {1, 2, 3, 4, 5, 6};
        outputStream.write(data);
        outputStream.close();

        ObsException obsException = new ObsException("mock ObsException.");
        ObsException eofException = new ObsException(
            "mock ObsException of EOF.");
        eofException.setResponseCode(OBSCommonUtils.EOF_CODE);
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);

        Mockito.doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(notFoundException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(eofException)
            .doCallRealMethod()
            .when(mockObsClient)
            .getObject(anyObject());

        FSDataInputStream inputStream = mockFs.open(testFile, 4096);
        byte[] buffer = new byte[1024];
        int bytes;
        try {
            int i = 0;
            while (true) {
                try {
                    bytes = inputStream.read(buffer, 0, 5);
                    break;
                } catch (AccessControlException e) {
                    assertTrue(i == 0 || i == 1);
                } catch (FileNotFoundException e) {
                    assertTrue(i == 2);
                }
                i++;
            }
            assertTrue(i == 3);
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
        assertTrue(bytes == -1);
    }

    @Test
    // 测试onReadFailure时对reopen的重试机制
    public void testOnReadFailure() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile, false);
        byte[] data = {0, 0, 0, 0, 0, 0};
        outputStream.write(data);
        outputStream.close();

        ObsException obsException = new ObsException("mock ObsException.");
        ObsException eofException = new ObsException(
            "mock ObsException of EOF.");
        eofException.setResponseCode(OBSCommonUtils.EOF_CODE);
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);
        SocketException connectionResetException = new SocketException(
            "mock Connection Reset Exception");
        ObsException socketException = new ObsException("connection reset ",
            connectionResetException);

        Mockito.doThrow(eofException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(eofException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(eofException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(notFoundException)
            .doThrow(eofException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(eofException)
            .doThrow(eofException)
            .doThrow(socketException)
            .doThrow(obsException)
            .doCallRealMethod()
            .when(mockObsClient)
            .getObject(anyObject());

        FSDataInputStream inputStream = mockFs.open(testFile, 4096);
        byte[] buffer = new byte[1024];
        int bytes;
        try {
            int i = 0;
            while (true) {
                try {
                    bytes = inputStream.read(buffer, 0, 5);
                    break;
                } catch (AccessControlException e) {
                    assertTrue(i == 0 || i == 1);
                } catch (FileNotFoundException e) {
                    assertTrue(i == 2);
                } catch (EOFException e) {
                    assertTrue(i == 3);
                }
                i++;
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
        assertTrue(bytes == -1);
    }

    @Test
    // 测试read(byte b[], int off, int len)接口重试机制，重试时对于EOF等特殊异常能正确识别
    public void testReadRetry() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile, false);
        byte[] data = {0, 0, 0, 0, 0, 0};
        outputStream.write(data);
        outputStream.close();

        ObsObject obsObject = new ObsObject();
        obsObject.setBucketName(fs.getBucket());
        obsObject.setObjectContent(new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        });

        InputStream mockInputStream = Mockito.spy(obsObject.getObjectContent());
        IOException ioException = new IOException("mock IOException");
        EOFException eofException = new EOFException("mock "
            + "EOFException");
        Mockito.doThrow(ioException)
            .doThrow(ioException)
            .doThrow(ioException)
            .doThrow(ioException)
            .doThrow(eofException)
            .doThrow(ioException)
            .doThrow(ioException)
            .doCallRealMethod()
            .when(mockInputStream)
            .read();

        obsObject.setObjectContent(mockInputStream);
        Mockito.doReturn(obsObject)
            .when(mockObsClient)
            .getObject(anyObject());

        FSDataInputStream inputStream = mockFs.open(testFile, 4096);
        byte[] buffer = new byte[1024];
        int bytes = -1;
        try {
            int i = 0;
            while (bytes < 0) {
                bytes = inputStream.read(buffer, 0, 5);
                if (bytes == -1) {
                    assertTrue(i == 0);
                } else {
                    assertTrue(i == 1);
                }
                i++;
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
        assertTrue(bytes == 5);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);

    }

    //read()
    @Test
    // 测试read()接口重试机制，重试时对于EOF等特殊异常能正确识别
    public void testReadRetry001() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile, false);
        byte[] data = {0, 0, 0, 0, 0, 0};
        outputStream.write(data);
        outputStream.close();

        ObsObject obsObject = new ObsObject();
        obsObject.setBucketName(fs.getBucket());
        obsObject.setObjectContent(new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        });

        InputStream mockInputStream = Mockito.spy(obsObject.getObjectContent());
        IOException ioException = new IOException("mock IOException");
        EOFException eofException = new EOFException("mock "
            + "EOFException");
        Mockito.doThrow(ioException)
            .doThrow(ioException)
            .doThrow(ioException)
            .doThrow(ioException)
            .doThrow(eofException)
            .doThrow(ioException)
            .doThrow(ioException)
            .doCallRealMethod()
            .when(mockInputStream)
            .read();

        obsObject.setObjectContent(mockInputStream);
        Mockito.doReturn(obsObject)
            .when(mockObsClient)
            .getObject(anyObject());

        FSDataInputStream inputStream = mockFs.open(testFile, 4096);
        int bytes = -1;
        try {
            int i = 0;
            while (bytes < 0) {
                bytes = inputStream.read();
                if (bytes == -1) {
                    assertTrue(i == 0);
                } else {
                    assertTrue(i == 1);
                }
                i++;
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
        assertTrue(bytes == 0);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);

    }

    //read(ByteBuffer byteBuffer)
    @Test
    // 测试read(ByteBuffer byteBuffer)接口重试机制，重试时对于EOF等特殊异常能正确识别
    public void testReadByteBufferRetry() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile, false);
        byte[] data = {0, 0, 0, 0, 0, 0};
        outputStream.write(data);
        outputStream.close();

        ObsObject obsObject = new ObsObject();
        obsObject.setBucketName(fs.getBucket());
        obsObject.setObjectContent(new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        });

        InputStream mockInputStream = Mockito.spy(obsObject.getObjectContent());
        IOException ioException = new IOException("mock IOException");
        EOFException eofException = new EOFException("mock "
            + "EOFException");
        Mockito.doThrow(ioException)
            .doThrow(ioException)
            .doThrow(ioException)
            .doThrow(ioException)
            .doThrow(eofException)
            .doThrow(ioException)
            .doThrow(ioException)
            .doCallRealMethod()
            .when(mockInputStream)
            .read();

        obsObject.setObjectContent(mockInputStream);
        Mockito.doReturn(obsObject)
            .when(mockObsClient)
            .getObject(anyObject());

        FSDataInputStream inputStream = mockFs.open(testFile, 4096);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
        int bytes = -1;
        try {
            int i = 0;
            while (bytes < 0) {
                bytes = inputStream.read(byteBuffer);
                if (bytes == -1) {
                    assertTrue(i == 0);
                } else {
                    assertTrue(i == 1);
                }
                i++;
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
        assertTrue(bytes == 1024);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }


    @Test
    //randomReadWithNewInputStream  前半部分重试机制
    public void testReadWithNewInputStreamRetry001() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile, false);
        byte[] data = {1, 2, 3, 4, 5, 6};
        outputStream.write(data);
        outputStream.close();

        ObsException obsException = new ObsException("mock ObsException.");
        ObsException eofException = new ObsException(
            "mock ObsException of EOF.");
        eofException.setResponseCode(OBSCommonUtils.EOF_CODE);
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);

        Mockito.doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(notFoundException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(eofException)
            .doCallRealMethod()
            .when(mockObsClient)
            .getObject(anyObject());

        FSDataInputStream inputStream = mockFs.open(testFile, 4096);
        byte[] buffer = new byte[1024];
        int bytes;
        try {
            int i = 0;
            while (true) {
                try {
                    bytes = inputStream.read(0, buffer, 0, 5);
                    break;
                } catch (AccessControlException e) {
                    assertTrue(i == 0 || i == 1);
                } catch (FileNotFoundException e) {
                    assertTrue(i == 2);
                }
                i++;
            }
            assertTrue(i == 3);
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
        assertTrue(bytes == -1);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    //randomReadWithNewInputStream  后半部分重试机制
    public void testReadWithNewInputStreamRetry002() throws Exception {
        Path testFile = getTestPath("test_file");
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
        FSDataOutputStream outputStream = fs.create(testFile, false);
        byte[] data = {0, 0, 0, 0, 0, 0};
        outputStream.write(data);
        outputStream.close();

        ObsObject obsObject = new ObsObject();
        obsObject.setBucketName(fs.getBucket());
        obsObject.setObjectContent(new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        });

        InputStream mockInputStream = Mockito.spy(obsObject.getObjectContent());
        IOException ioException = new IOException("mock IOException");
        EOFException eofException = new EOFException("mock "
            + "EOFException");
        Mockito.doThrow(ioException)
            .doThrow(ioException)
            .doThrow(ioException)
            .doThrow(ioException)
            .doThrow(eofException)
            .doThrow(ioException)
            .doThrow(ioException)
            .doCallRealMethod()
            .when(mockInputStream)
            .read();

        obsObject.setObjectContent(mockInputStream);
        Mockito.doReturn(obsObject)
            .when(mockObsClient)
            .getObject(anyObject());

        FSDataInputStream inputStream = mockFs.open(testFile, 4096);
        byte[] buffer = new byte[1024];
        int bytes = -1;
        try {
            int i = 0;
            while (bytes < 0) {
                bytes = inputStream.read(0, buffer, 0, 5);
                if (bytes == -1) {
                    assertTrue(i == 0);
                } else {
                    assertTrue(i == 1);
                }
                i++;
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
        assertTrue(bytes == 5);
        OBSFSTestUtil.deletePathRecursive(fs, testFile);
    }

    @Test
    // 测试delete接口重试机制，重试时对于404等特殊异常能正确识别
    public void testDeleteObject() throws Exception {
        ObsException obsException = new ObsException("mock ObsException.");
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);
        ObsException conflictException = new ObsException("mock "
            + "conflict exception.");
        conflictException.setResponseCode(OBSCommonUtils.CONFLICT_CODE);

        Mockito.doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(notFoundException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(conflictException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doCallRealMethod()
            .when(mockObsClient)
            .deleteObject(anyString(), anyString());

        int i = 0;
        while (true) {
            try {
                OBSCommonUtils.deleteObject(mockFs,
                    testFile.toString().substring(1));
                break;
            } catch (AccessControlException e) {
                assertTrue(i == 0 || i == 1);
            } catch (FileNotFoundException e) {
                assertTrue(i == 2);
            } catch (OBSFileConflictException e) {
                assertTrue(i == 3);
            }
            i++;
        }
        assertTrue(i == 4);
    }

    @Test
    // 测试对象桶CopyFile接口重试机制，重试时对于403等特殊异常能正确识别
    public void testCopyFile() throws Exception {
        if (fs.isFsBucket()) {
            return;
        }
        Path srcFile = getTestPath("srcFile");
        Path destFile = getTestPath("destFile");
        FSDataOutputStream os = fs.create(srcFile);
        OBSFSTestUtil.writeData(os, 1024 * 1024);
        os.close();

        ObsException obsException = new ObsException("mock ObsException.");
        ObsException unauthorizedException = new ObsException("mock "
            + "unauthorized exception.");
        unauthorizedException.setResponseCode(OBSCommonUtils.UNAUTHORIZED_CODE);
        ObsException forbiddenException = new ObsException("mock "
            + "forbidden exception.");
        forbiddenException.setResponseCode(OBSCommonUtils.FORBIDDEN_CODE);
        ObsException notFoundException = new ObsException("mock "
            + "not found exception.");
        notFoundException.setResponseCode(OBSCommonUtils.NOT_FOUND_CODE);
        ObsException conflictException = new ObsException("mock "
            + "conflict exception.");
        conflictException.setResponseCode(OBSCommonUtils.CONFLICT_CODE);

        Mockito.doThrow(obsException)
            .doThrow(obsException)
            .doThrow(unauthorizedException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(forbiddenException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(notFoundException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(conflictException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doThrow(obsException)
            .doCallRealMethod()
            .when(mockObsClient)
            .copyObject(anyObject());

        Mockito.doCallRealMethod()
            .when(mockObsClient)
            .getObjectMetadata(anyObject());

        Mockito.doReturn(5 * 1024 * 1024L)
            .when(mockFs).getCopyPartSize();

        int i = 0;
        while (true) {
            try {
                OBSObjectBucketUtils.copyFile(mockFs,
                    srcFile.toString().substring(1),
                    destFile.toString().substring(1), 10);
                break;
            } catch (AccessControlException e) {
                assertTrue(i == 0 || i == 1);
            } catch (FileNotFoundException e) {
                assertTrue(i == 2);
            } catch (OBSFileConflictException e) {
                assertTrue(i == 3);
            }
            i++;
        }
        assertTrue(i == 4);
    }

    @Test
    // 测试文件桶递归删除时，服务端返回409，在OBSA侧执行重试
    public void testDeleteNotEmptyDirRecursively() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }

        // 1、构造多级目录结构
        Path dir01 = getTestPath("dir01/");
        Path subDir01 = getTestPath("dir01/subDir01/");
        Path subFile01 = getTestPath("dir01/subFile01");
        fs.mkdirs(dir01);
        fs.mkdirs(subDir01);
        FSDataOutputStream os = fs.create(subFile01);
        OBSFSTestUtil.writeData(os, 1024 * 1024);
        os.close();

        // 2、删除对象注入FileConflictException异常
        ObsException conflictException = new ObsException("mock "
            + "conflict exception.");
        conflictException.setResponseCode(OBSCommonUtils.CONFLICT_CODE);

        Mockito.doThrow(conflictException)
            .doThrow(conflictException)
            .doThrow(conflictException)
            .doCallRealMethod()
            .when(mockObsClient)
            .deleteObject(anyString(), anyString());

        Mockito.doThrow(conflictException)
            .doThrow(conflictException)
            .doThrow(conflictException)
            .doCallRealMethod()
            .when(mockObsClient)
            .deleteObjects(anyObject());

        // OBSCommonUtils.RETRY_MAXTIME = 10;
        // // 3、递归删除一级目录，重试超时，删除失败
        // boolean hasException = false;
        // try {
        //     mockFs.delete(dir01, true);
        // } catch (Exception e) {
        //     hasException = true;
        // }
        // assertTrue("delete folder should has exception", hasException);
        //
        // OBSCommonUtils.RETRY_MAXTIME = 180000;
        // 4、递归删除一级目录，重试多次成功
        boolean hasException = false;
        try {
            mockFs.delete(dir01, true);
        } catch (Exception e) {
            hasException = true;
        }
        assertFalse("delete folder should not has exception", hasException);
    }
}
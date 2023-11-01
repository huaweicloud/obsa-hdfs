package org.apache.hadoop.fs.obs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import com.obs.services.model.fs.FSStatusEnum;
import com.obs.services.model.fs.GetAttributeRequest;
import com.obs.services.model.fs.GetBucketFSStatusResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.security.AccessControlException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

@RunWith(MockitoJUnitRunner.class)
public class ITestOBSRetryMechanism2 {
    private OBSFileSystem fs;
    private ObsClient obsClient;
    private OBSFileSystem mockFs;
    private ObsClient mockObsClient;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private Path testFile = getTestPath("testFile");

    private Path testDir = getTestPath("testDir");

    private static byte[] dataSet = ContractTestUtils.dataset(16, 0, 10);


    ObsException exception400;
    ObsException exception403;
    ObsException exception404;
    ObsException exception409;
    ObsException exception416;

    ObsException exception500;
    ObsException exception503;

    IOException[] noRetryIOExceptions;
    IOException[] retryIOExceptions;

    HashMap<Exception, Class> noRetryExceptions = new HashMap<>();
    HashMap<Exception, Class> retryExceptions = new HashMap<>();

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
        conf.setLong(OBSConstants.MULTIPART_SIZE,1024*1024L);
        fs = OBSTestUtils.createTestFileSystem(conf);
        setRetryParam();
        obsClient = fs.getObsClient();
        initTestEnv();
        initMock();

        exception400 = new ObsException("mock 400 exception");
        exception400.setResponseCode(400);
        exception403 = new ObsException("mock 403 exception");
        exception403.setResponseCode(403);
        exception404 = new ObsException("mock 404 exception");
        exception404.setResponseCode(404);
        exception409 = new ObsException("mock 409 exception");
        exception409.setResponseCode(409);

        exception416 = new ObsException("mock 416 exception");
        exception416.setResponseCode(416);

        exception500 = new ObsException("mock 500 exception");
        exception500.setResponseCode(500);
        exception503 = new ObsException("mock 503 exception");
        exception503.setResponseCode(503);
        exception503.setErrorCode("GetQosTokenException");

        noRetryIOExceptions = new IOException[] {
            new OBSIllegalArgumentException("mock OBSIllegalArgumentException"),
            new AccessControlException("mock AccessControlException"),
            new FileNotFoundException("mock FileNotFoundException"),
            new OBSFileConflictException("mock OBSFileConflictException")
        };
        retryIOExceptions = new IOException[] {
            new OBSQosException("mock OBSQosException", exception503),
            new OBSIOException("mock OBSIOException", exception500)
        };

        noRetryExceptions.put(exception400, OBSIllegalArgumentException.class);
        noRetryExceptions.put(exception403, AccessControlException.class);
        noRetryExceptions.put(exception404, FileNotFoundException.class);
        noRetryExceptions.put(exception409, OBSFileConflictException.class);
        noRetryExceptions.put(exception416, EOFException.class);

        retryExceptions.put(exception500, OBSIOException.class);
        retryExceptions.put(exception503, OBSQosException.class);
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

        // mockFs_retryFail = Mockito.spy(fs_retryFail);
        // mockObsClient_retryFail = Mockito.spy(obsClient_retryFail);
        // Whitebox.setInternalState(mockFs_retryFail, obsClient_retryFail, mockObsClient_retryFail);
    }

    @Test
    public void testGetFileStatus() throws IOException {
        for (IOException ex : noRetryIOExceptions) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockFs)
                .innerGetFileStatus(anyObject());
            try {
                mockFs.getFileStatus(testFile);
            } catch (IOException e) {
                if (e.getMessage().contains("mock OBSFileConflictException")) {
                    Assert.assertTrue("testGetFileStatus",e.getClass() == FileNotFoundException.class);
                } else {
                    Assert.assertTrue("testGetFileStatus",e.getClass() == ex.getClass());
                }
            }
        }

        //重试成功
        for (IOException ex : retryIOExceptions) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockFs)
                .innerGetFileStatus(anyObject());
            try {
                mockFs.getFileStatus(testFile);
            } catch (IOException e) {
                Assert.assertTrue("testGetFileStatus",false);
            }
        }


        //重试失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockFs)
                .innerGetFileStatus(anyObject());
            try {
                mockFs.getFileStatus(testFile);
            } catch (IOException e) {
                Assert.assertTrue("testGetFileStatus",e.getClass() == retryExceptions.get(ex));
            }
        }
    }

    @Test
    public void testWriteByUploadPart() {
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .uploadPart(anyObject());
            try {
                FSDataOutputStream outputStream = mockFs.create(getTestPath("testCreateByUploadPart"));
                OBSFSTestUtil.writeData(outputStream, 6* 1024 * 1024);
            } catch (IOException e) {
                Assert.assertTrue("upload part",e.getMessage().contains("write has error"));
            }
        }
        //重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .uploadPart(anyObject());

            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .initiateMultipartUpload(anyObject());

            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .completeMultipartUpload(anyObject());

            try {
                FSDataOutputStream outputStream = mockFs.create(getTestPath("testCreateByUploadPart"));
                OBSFSTestUtil.writeData(outputStream, 6* 1024 * 1024);
                outputStream.close();
            } catch (IOException e) {
                Assert.assertTrue("upload part",false);
            }
        }

    }

    @Test
    public void testWriteByUploadPartWithInitFail() {
        setNewRetryParam();
        OBSWriteOperationHelper writeHelper = fs.getWriteHelper();
        Whitebox.setInternalState(writeHelper, obsClient, mockObsClient);
        //重试失败
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .initiateMultipartUpload(anyObject());
            try {
                FSDataOutputStream outputStream = mockFs.create(getTestPath("testCreateByUploadPart"));
                OBSFSTestUtil.writeData(outputStream, 6* 1024 * 1024);
            } catch (IOException e) {
                Assert.assertTrue("upload part",
                    e.getMessage().contains(OBSOperateAction.initMultiPartUpload.toString()));
            }
        }
    }

    @Test
    public void testWriteByUploadPartWithCompleteFail() {
        setNewRetryParam();
        OBSWriteOperationHelper writeHelper = fs.getWriteHelper();
        Whitebox.setInternalState(writeHelper, obsClient, mockObsClient);

        //重试第3次失败
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .completeMultipartUpload(anyObject());
            try {
                FSDataOutputStream outputStream = mockFs.create(getTestPath("testCreateByUploadPart"));
                OBSFSTestUtil.writeData(outputStream, 6* 1024 * 1024);
                outputStream.close();
            } catch (IOException e) {
                Assert.assertTrue("upload part",e.getMessage().contains("completeMultipartUpload"));
            }
        }
    }

    @Test
    public void testWriteByUploadPartWithUploadFail() {
        setNewRetryParam();
        OBSWriteOperationHelper writeHelper = fs.getWriteHelper();
        Whitebox.setInternalState(writeHelper, obsClient, mockObsClient);

        //重试失败
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .uploadPart(anyObject());
            try {
                FSDataOutputStream outputStream = mockFs.create(getTestPath("testCreateByUploadPart"));
                OBSFSTestUtil.writeData(outputStream, 6* 1024 * 1024);
            } catch (IOException e) {
                Assert.assertTrue("upload part",e.getMessage().contains("write has error"));
            }
        }
    }

    @Test
    public void testFsAppend() throws IOException {
        if (!fs.isFsBucket()) {
            return;
        }
        FSDataOutputStream out = fs.create(getTestPath("testAppendFile"));
        out.write("123456".getBytes());
        out.close();

        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .writeFile(anyObject());
            FSDataOutputStream outputStream = null;
            try {
                outputStream = mockFs.append(getTestPath("testAppendFile"));
                outputStream.write("123456".getBytes());
                outputStream.close();
            } catch (IOException e) {
                Assert.assertTrue("testAppend",e.getClass()== noRetryExceptions.get(ex));
            }
        }

        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .writeFile(anyObject());
            try {
                FSDataOutputStream outputStream = mockFs.append(getTestPath("testAppendFile"));
                outputStream.write("123456".getBytes());
                outputStream.close();
            } catch (IOException e) {
                Assert.assertTrue("testAppend",false);
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .writeFile(anyObject());
            try {
                FSDataOutputStream outputStream = mockFs.append(getTestPath("testAppendFile"));
                outputStream.write("123456".getBytes());
                outputStream.close();
            } catch (IOException e) {
                Assert.assertTrue("testAppend", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    public void testFsAppendByHflush() {
        if (!fs.isFsBucket()) {
            return;
        }
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .writeFile(anyObject());
            try {
                FSDataOutputStream outputStream = mockFs.create(getTestPath("testAppendByHflush"));
                outputStream.write("123456".getBytes());
                outputStream.hflush();
                outputStream.write("123456".getBytes());
                outputStream.close();
            } catch (IOException e) {
                Assert.assertTrue("testAppendByHflush",e.getClass()== noRetryExceptions.get(ex));
            }
        }

        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .writeFile(anyObject());
            try {
                FSDataOutputStream outputStream = mockFs.create(getTestPath("testAppendByHflush"));
                outputStream.write("123456".getBytes());
                outputStream.hflush();
                outputStream.write("123456".getBytes());
                outputStream.close();
            } catch (IOException e) {
                Assert.assertTrue("testAppendByHflush",false);
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .writeFile(anyObject());
            try {
                FSDataOutputStream outputStream = mockFs.create(getTestPath("testAppendByHflush"));
                outputStream.write("123456".getBytes());
                outputStream.hflush();
                outputStream.write("123456".getBytes());
                outputStream.close();
            } catch (IOException e) {
                Assert.assertTrue("testAppendByHflush", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    public void testWriteByPutObject() {
        OBSWriteOperationHelper writeHelper = fs.getWriteHelper();
        Whitebox.setInternalState(writeHelper, fs, mockFs);
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .putObject(anyObject());
            try {
                FSDataOutputStream outputStream = mockFs.create(getTestPath("testCreateByPutObject"));
                OBSFSTestUtil.writeData(outputStream, 4 * 1024 * 1024);
                outputStream.close();
            } catch (IOException e) {
                Assert.assertTrue("testCreateByPutObject",e.getClass()== noRetryExceptions.get(ex));
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .putObject(anyObject());
            try {
                FSDataOutputStream outputStream = mockFs.create(getTestPath("testCreateByPutObject"));
                OBSFSTestUtil.writeData(outputStream, 4 * 1024 * 1024);
                outputStream.close();
            } catch (IOException e) {
                Assert.assertTrue("testCreateByPutObject",false);
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .putObject(anyObject());
            try {
                FSDataOutputStream outputStream = mockFs.create(getTestPath("testCreateByPutObject"));
                OBSFSTestUtil.writeData(outputStream, 4 * 1024 * 1024);
                outputStream.close();
            } catch (IOException e) {
                Assert.assertTrue("testCreateByPutObject", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    public void testIsFolderEmpty() throws Exception {
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .listObjects((ListObjectsRequest) anyObject());
            try {
                OBSCommonUtils.isFolderEmpty(mockFs, testDir.toString().substring(1));
            } catch (IOException e) {
                Assert.assertTrue("testIsFolderEmpty",e.getClass()== noRetryExceptions.get(ex));
            }
        }
        // 重试成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .listObjects((ListObjectsRequest) anyObject());
            try {
                OBSCommonUtils.isFolderEmpty(mockFs, testDir.toString().substring(1));
            } catch (IOException e) {
                Assert.assertTrue("testIsFolderEmpty",false);
            }
        }

        //重试失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .listObjects((ListObjectsRequest) anyObject());
            try {
                OBSCommonUtils.isFolderEmpty(mockFs, testDir.toString().substring(1));
            } catch (IOException e) {
                Assert.assertTrue("testIsFolderEmpty", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    public void testObjectMkdir() {
        if (fs.isFsBucket()) {
            return;
        }
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .putObject(anyObject());
            try {
                OBSObjectBucketUtils.createEmptyObject(mockFs,
                    OBSCommonUtils.pathToKey(fs, getTestPath("testMkdirByCreateEmptyObject")));
            } catch (IOException e) {
                Assert.assertTrue("testMkdirByCreateEmptyObject",e.getClass()== noRetryExceptions.get(ex));
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .putObject(anyObject());
            try {
                OBSObjectBucketUtils.createEmptyObject(mockFs,
                    OBSCommonUtils.pathToKey(fs, getTestPath("testMkdirByCreateEmptyObject")));
            } catch (IOException e) {
                Assert.assertTrue("testMkdirByCreateEmptyObject",false);
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .putObject(anyObject());
            try {
                OBSObjectBucketUtils.createEmptyObject(mockFs,
                    OBSCommonUtils.pathToKey(fs, getTestPath("testMkdirByCreateEmptyObject")));
            } catch (IOException e) {
                Assert.assertTrue("testMkdirByCreateEmptyObject", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    // 测试FsCreateFolder重试机制，重试时对于403等特殊异常能正确识别
    public void testFsMkdir() throws IOException {
        if (!fs.isFsBucket()) {
            return;
        }
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .newFolder(anyObject());
            try {
                OBSPosixBucketUtils.fsCreateFolder(mockFs,
                    OBSCommonUtils.pathToKey(fs, getTestPath("testMkdirByFsCreateFolder")));
            } catch (IOException e) {
                Assert.assertTrue("testMkdirByFsCreateFolder",e.getClass()== noRetryExceptions.get(ex));
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .newFolder(anyObject());
            try {
                OBSPosixBucketUtils.fsCreateFolder(mockFs,
                    OBSCommonUtils.pathToKey(fs, getTestPath("testMkdirByFsCreateFolder")));
            } catch (IOException e) {
                Assert.assertTrue("testMkdirByFsCreateFolder",false);
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .newFolder(anyObject());
            try {
                OBSPosixBucketUtils.fsCreateFolder(mockFs,
                    OBSCommonUtils.pathToKey(fs, getTestPath("testMkdirByFsCreateFolder")));
            } catch (IOException e) {
                Assert.assertTrue("testMkdirByFsCreateFolder", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    public void testListStatusByListObjects() throws Exception {
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .listObjects((ListObjectsRequest) anyObject());
            try {
                ListObjectsRequest request = new ListObjectsRequest();
                request.setBucketName(mockFs.getBucket());
                request.setPrefix(testRootPath.substring(1));
                request.setDelimiter("/");
                request.setMaxKeys(1000);
                OBSCommonUtils.listObjects(mockFs, request);
            } catch (IOException e) {
                Assert.assertTrue("testListStatusByListObjects",e.getClass()== noRetryExceptions.get(ex));
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .listObjects((ListObjectsRequest) anyObject());
            try {
                ListObjectsRequest request = new ListObjectsRequest();
                request.setBucketName(mockFs.getBucket());
                request.setPrefix(testRootPath.substring(1));
                request.setDelimiter("/");
                request.setMaxKeys(1000);
                OBSCommonUtils.listObjects(mockFs, request);
            } catch (IOException e) {
                Assert.assertTrue("testListStatusByListObjects",false);
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .listObjects((ListObjectsRequest) anyObject());
            try {
                ListObjectsRequest request = new ListObjectsRequest();
                request.setBucketName(mockFs.getBucket());
                request.setPrefix(testRootPath.substring(1));
                request.setDelimiter("/");
                request.setMaxKeys(1000);
                OBSCommonUtils.listObjects(mockFs, request);
            } catch (IOException e) {
                Assert.assertTrue("testListStatusByListObjects", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    public void testListStatusByListObjectsRecursively() throws Exception {
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .listObjects((ListObjectsRequest) anyObject());
            try {
                ListObjectsRequest request = new ListObjectsRequest();
                request.setBucketName(mockFs.getBucket());
                request.setPrefix(testRootPath.substring(1));
                request.setMaxKeys(1000);
                OBSCommonUtils.listObjects(mockFs, request);
            } catch (IOException e) {
                Assert.assertTrue("testListStatusByListObjects",e.getClass()== noRetryExceptions.get(ex));
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .listObjects((ListObjectsRequest) anyObject());
            try {
                ListObjectsRequest request = new ListObjectsRequest();
                request.setBucketName(mockFs.getBucket());
                request.setPrefix(testRootPath.substring(1));
                request.setMaxKeys(1000);
                OBSCommonUtils.listObjects(mockFs, request);
            } catch (IOException e) {
                Assert.assertTrue("testListStatusByListObjects",false);
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .listObjects((ListObjectsRequest) anyObject());
            try {
                ListObjectsRequest request = new ListObjectsRequest();
                request.setBucketName(mockFs.getBucket());
                request.setPrefix(testRootPath.substring(1));
                request.setMaxKeys(1000);
                OBSCommonUtils.listObjects(mockFs, request);
            } catch (IOException e) {
                Assert.assertTrue("testListStatusByListObjects", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    // 测试ContinueListObjects重试机制，重试时对于403等特殊异常能正确识别
    public void testListStatusByContinueListObjects() throws Exception {
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .listObjects((ListObjectsRequest) anyObject());
            try {
                ObjectListing objectListing = new ObjectListing(null, null,
                    mockFs.getBucket(), true, testRootPath.substring(1), null,
                    1000, "/", "/", null);
                OBSCommonUtils.continueListObjects(mockFs, objectListing);
            } catch (IOException e) {
                Assert.assertTrue("testListStatusByContinueListObjects",e.getClass()== noRetryExceptions.get(ex));
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .listObjects((ListObjectsRequest) anyObject());
            try {
                ObjectListing objectListing = new ObjectListing(null, null,
                    mockFs.getBucket(), true, testRootPath.substring(1), null,
                    1000, "/", "/", null);
                OBSCommonUtils.continueListObjects(mockFs, objectListing);
            } catch (IOException e) {
                Assert.assertTrue("testListStatusByContinueListObjects",false);
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .listObjects((ListObjectsRequest) anyObject());
            try {
                ObjectListing objectListing = new ObjectListing(null, null,
                    mockFs.getBucket(), true, testRootPath.substring(1), null,
                    1000, "/", "/", null);
                OBSCommonUtils.continueListObjects(mockFs, objectListing);
            } catch (IOException e) {
                Assert.assertTrue("testListStatusByContinueListObjects", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    // 与testListStatusByListObjectsRecursively的测试点重复，不用再重复测试
    public void testListStatusByContinueListObjectsRecursively() {
    }

    @Test
    public void testFsTruncate() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }

        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .truncateObject(anyObject());
            try {
                OBSPosixBucketUtils.innerFsTruncateWithRetry(mockFs, testFile, 3);
            } catch (IOException e) {
                Assert.assertTrue("testFsTruncate",e.getClass()== noRetryExceptions.get(ex));
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .truncateObject(anyObject());
            try {
                OBSPosixBucketUtils.innerFsTruncateWithRetry(mockFs, testFile, 3);
            } catch (IOException e) {
                Assert.assertTrue("testFsTruncate",false);
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .truncateObject(anyObject());
            try {
                OBSPosixBucketUtils.innerFsTruncateWithRetry(mockFs, testFile, 3);
            } catch (IOException e) {
                Assert.assertTrue("testFsTruncate", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    public void testFsRenameFile() throws Exception {
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


        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .renameFile(anyObject());
            try {
                boolean success = OBSPosixBucketUtils.innerFsRenameWithRetry(mockFs,
                    testFileSrc, testFileDst, srcKey, dstKey);
                //404 FileNotFoundException rename将返回false
                assertFalse(success);
            } catch (IOException e) {
                Assert.assertTrue("testFsRenameFile",e.getClass()== noRetryExceptions.get(ex));
            }
        }

        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .renameFile(anyObject());
            try {
                boolean success = OBSPosixBucketUtils.innerFsRenameWithRetry(mockFs,
                    testFileSrc, testFileDst, srcKey, dstKey);
                //第1次循环重试成功，第2次循环dst已经存在但是src不存在了,根据对FileNotFoundException的处理将返回true
                assertTrue(success);
            } catch (IOException e) {
                Assert.assertTrue("testFsRenameFile",e.getClass() == FileNotFoundException.class );
            }
        }
    }

    @Test
    public void testFsRenameFileWithFail() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        setNewRetryParam();
        Path testFileSrc = getTestPath("test_file_src");
        FSDataOutputStream outputStream = fs.create(testFileSrc, false);
        byte[] data = {0, 0, 0, 0, 0, 0};
        outputStream.write(data);
        outputStream.close();
        Path testFileDst = getTestPath("test_file_dst");
        String srcKey = OBSCommonUtils.pathToKey(fs, testFileSrc);
        String dstKey = OBSCommonUtils.pathToKey(fs, testFileDst);
        //重试第3次失败
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .renameFile(anyObject());
            try {
                OBSPosixBucketUtils.innerFsRenameWithRetry(mockFs,
                    testFileSrc, testFileDst, srcKey, dstKey);
            } catch (IOException e) {
                Assert.assertTrue("testFsRenameFile", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    public void testFsRenameFolder() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }

        Path testDirSrc = getTestPath("test_dir_src");
        fs.mkdirs(testDirSrc);
        Path testDirDst = getTestPath("test_dir_dst");
        String srcKey = OBSCommonUtils.pathToKey(fs, testDirSrc);
        String dstKey = OBSCommonUtils.pathToKey(fs, testDirDst);

        for (Exception ex : noRetryExceptions.keySet()) {
            if (((ObsException)ex).getResponseCode() == 403) {
                Mockito.doThrow(ex)
                    .doThrow(ex)
                    .doCallRealMethod()
                    .when(mockObsClient)
                    .renameFile(anyObject());
            } else {
                Mockito.doThrow(ex)
                    .doCallRealMethod()
                    .when(mockObsClient)
                    .renameFile(anyObject());
            }

            try {
                boolean success =  OBSPosixBucketUtils.innerFsRenameWithRetry(mockFs, testDirSrc, testDirDst,
                    srcKey, dstKey);
                //404 FileNotFoundException rename将返回false
                assertFalse(success);
            } catch (IOException e) {
                Assert.assertTrue("testFsRenameFolder",e.getClass()== noRetryExceptions.get(ex));
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .renameFile(anyObject());
            try {
                boolean success =  OBSPosixBucketUtils.innerFsRenameWithRetry(mockFs, testDirSrc, testDirDst,
                    srcKey, dstKey);
                //第1次循环重试成功，第2次循环dst已经存在但是src不存在了
                assertTrue(success);
            } catch (IOException e) {
                Assert.assertTrue("testFsRenameFolder",e.getClass() == FileNotFoundException.class );
            }
        }
    }

    @Test
    public void testFsRenameFolderWithFail() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        setNewRetryParam();
        Path testDirSrc = getTestPath("test_dir_src");
        fs.mkdirs(testDirSrc);
        Path testDirDst = getTestPath("test_dir_dst");
        String srcKey = OBSCommonUtils.pathToKey(fs, testDirSrc);
        String dstKey = OBSCommonUtils.pathToKey(fs, testDirDst);

        //重试第3次失败
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .renameFile(anyObject());
            try {
                OBSPosixBucketUtils.innerFsRenameWithRetry(mockFs, testDirSrc, testDirDst, srcKey, dstKey);
            } catch (IOException e) {
                Assert.assertTrue("testFsRenameFolder", e.getClass() == retryExceptions.get(ex));
            }
        }
    }

    @Test
    public void testGetBucketStatus() {
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .getBucketFSStatus(anyObject());
            try {
                OBSCommonUtils.getBucketFsStatus(mockObsClient, fs.getBucket());
            } catch (IOException e) {
                Assert.assertTrue("testGetBucketStatus",e.getClass()== noRetryExceptions.get(ex));
            }
        }
        // 重试成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .getBucketFSStatus(anyObject());
            try {
                OBSCommonUtils.getBucketFsStatus(mockObsClient, fs.getBucket());
            } catch (IOException e) {
                Assert.assertTrue("testGetBucketStatus",false);
            }
        }

        //重试失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .getBucketFSStatus(anyObject());
            try {
                OBSCommonUtils.getBucketFsStatus(mockObsClient, fs.getBucket());
            } catch (IOException e) {
                Assert.assertTrue("testGetBucketStatus", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    public void testReadWhenSeekInStreamException() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile);
        outputStream.write("123456789".getBytes());
        outputStream.close();
        byte[] buffer = new byte[6];

        //读IO异常
        IOException ioException = new IOException("mock IOException");

        ObsObject obsObject = new ObsObject();
        obsObject.setBucketName(fs.getBucket());
        obsObject.setObjectContent(new InputStream() {
            @Override
            public int available() throws IOException {
                throw new IOException("mock IOException");
            }

            @Override
            public int read(byte b[], int off, int len) throws IOException {
                System.arraycopy("123456789".getBytes(),0, b, off, len);
                return len;
            }
            @Override
            public int read() throws IOException {
                return 1;
            }
        });

        InputStream mockInputStream = Mockito.spy(obsObject.getObjectContent());
        obsObject.setObjectContent(mockInputStream);
        Mockito.doReturn(obsObject)
            .when(mockObsClient)
            .getObject(anyObject());


        // // 重试成功
        // Mockito.doThrow(ioException)
        //     .doThrow(ioException)
        //     .doCallRealMethod()
        //     .when(mockInputStream)
        //     .read(anyObject(),anyInt(),anyInt());
        FSDataInputStream inputStream = null;
        try {
            inputStream = mockFs.open(testFile, 4096);
            inputStream.read(buffer, 0, 3);
            Assert.assertTrue("testReadWithBytes", "123".equals(new String(buffer).substring(0,3)));

            inputStream.read(4, buffer, 0, 5);
            Assert.assertTrue("testReadByRandom", "12345".equals(new String(buffer).substring(0,5)));
        } catch (IOException e) {
            Assert.assertTrue("testReadWithBytes",e.getClass()== ioException.getClass());
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }


    @Test
    public void testReadByBytes() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile);
        outputStream.write("123456".getBytes());
        outputStream.close();
        byte[] buffer = new byte[6];

        //obsclient异常
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;

            try {
                inputStream = mockFs.open(testFile, 4096);
                //EOF异常
                int read = inputStream.read(buffer, 0, 5);
                Assert.assertTrue("testReadWithBytes",read == -1);
            } catch (IOException e) {
                Assert.assertTrue("testReadWithBytes",e.getClass()== noRetryExceptions.get(ex));
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;
            try {
                inputStream = mockFs.open(testFile, 4096);
                inputStream.read(buffer, 0, 5);
                Assert.assertTrue("testReadWithBytes", "12345".equals(new String(buffer).substring(0,5)));
            } catch (IOException e) {
                Assert.assertTrue("testReadWithBytes",false);
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;
            try {
                inputStream = mockFs.open(testFile, 4096);
                inputStream.read(buffer, 0, 5);
            } catch (IOException e) {
                Assert.assertTrue("testReadWithBytes",e.getClass()== retryExceptions.get(ex));
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }
    }

    @Test
    public void testReadByBytesWithIO() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile);
        outputStream.write("123456".getBytes());
        outputStream.close();
        byte[] buffer = new byte[6];

        //读IO异常
        IOException ioException = new IOException("mock IOException");
        IOException eofException = new EOFException("mock EOFException");

        ObsObject obsObject = new ObsObject();
        obsObject.setBucketName(fs.getBucket());
        obsObject.setObjectContent(new InputStream() {
            @Override
            public int read(byte b[], int off, int len) throws IOException {
                System.arraycopy("123456".getBytes(),0, b, off, len);
                return len;
            }
            @Override
            public int read() throws IOException {
                return 1;
            }
        });

        InputStream mockInputStream = Mockito.spy(obsObject.getObjectContent());
        obsObject.setObjectContent(mockInputStream);
        Mockito.doReturn(obsObject)
            .when(mockObsClient)
            .getObject(anyObject());

        // 重试成功
        Mockito.doThrow(ioException)
            .doThrow(ioException)
            .doCallRealMethod()
            .when(mockInputStream)
            .read(anyObject(),anyInt(),anyInt());
        FSDataInputStream inputStream = null;
        try {
            inputStream = mockFs.open(testFile, 4096);
            inputStream.read(buffer, 0, 5);
            Assert.assertTrue("testReadWithBytes", "12345".equals(new String(buffer).substring(0,5)));
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }

        // 重试失败
        setNewRetryParam();
        Mockito.doThrow(ioException)
            .when(mockInputStream)
            .read(anyObject(),anyInt(),anyInt());
        try {
            inputStream = mockFs.open(testFile, 4096);
            inputStream.read(buffer, 0, 5);
        } catch (IOException e) {
            Assert.assertTrue("testReadWithBytes",e.getClass()== ioException.getClass());
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }


    @Test
    public void testReadByOneByte() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile);
        outputStream.write("123456".getBytes());
        outputStream.close();

        //obsclient异常
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;
            try {
                inputStream = mockFs.open(testFile, 4096);
                //EOF异常
                int read = inputStream.read();
                Assert.assertTrue("testReadWithOneByte",read == -1);
            } catch (IOException e) {
                Assert.assertTrue("testReadWithOneByte",e.getClass()== noRetryExceptions.get(ex));
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;
            try {
                inputStream = mockFs.open(testFile, 4096);
                int read = inputStream.read();
                Assert.assertTrue("", Integer.valueOf("123456".charAt(0))==read);
            } catch (IOException e) {
                Assert.assertTrue("testReadWithOneByte",false);
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;
            try {
                inputStream = mockFs.open(testFile, 4096);
                inputStream.read();
            } catch (IOException e) {
                Assert.assertTrue("testReadWithOneByte",e.getClass()== retryExceptions.get(ex));
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }

        // //读IO异常
        // IOException ioException = new IOException("mock IOException");
        // IOException eofException = new EOFException("mock EOFException");
        //
        // ObsObject obsObject = new ObsObject();
        // obsObject.setBucketName(fs.getBucket());
        // obsObject.setObjectContent(new InputStream() {
        //     @Override
        //     public int read() throws IOException {
        //         return 1;
        //     }
        // });
        //
        // InputStream mockInputStream = Mockito.spy(obsObject.getObjectContent());
        // obsObject.setObjectContent(mockInputStream);
        // Mockito.doReturn(obsObject)
        //     .when(mockObsClient)
        //     .getObject(anyObject());
        //
        // // 重试失败
        // Mockito.doThrow(ioException)
        //     .doThrow(ioException)
        //     .doThrow(ioException)
        //     .doCallRealMethod()
        //     .when(mockInputStream)
        //     .read();
        // FSDataInputStream inputStream = null;
        // try {
        //     inputStream = mockFs.open(testFile, 4096);
        //     inputStream.read();
        // } catch (IOException e) {
        //     Assert.assertTrue("testReadWithOneByte",e.getClass()== ioException.getClass());
        // } finally {
        //     if (inputStream != null) {
        //         inputStream.close();
        //     }
        // }
        //
        // // 重试成功
        // Mockito.doThrow(ioException)
        //     .doThrow(ioException)
        //     .doCallRealMethod()
        //     .when(mockInputStream)
        //     .read();
        // inputStream = null;
        // try {
        //     inputStream = mockFs.open(testFile, 4096);
        //     int read = inputStream.read();
        //     Assert.assertTrue("testReadWithOneByte", 1 == read);
        // } catch (IOException e) {
        //     Assert.assertTrue("testReadWithOneByte",e.getClass()== ioException.getClass());
        // } finally {
        //     if (inputStream != null) {
        //         inputStream.close();
        //     }
        // }
    }

    @Test
    public void testReadByOneByteWithIO() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile);
        outputStream.write("123456".getBytes());
        outputStream.close();


        //读IO异常
        IOException ioException = new IOException("mock IOException");
        IOException eofException = new EOFException("mock EOFException");

        ObsObject obsObject = new ObsObject();
        obsObject.setBucketName(fs.getBucket());
        obsObject.setObjectContent(new InputStream() {
            @Override
            public int read() throws IOException {
                return 1;
            }
        });

        InputStream mockInputStream = Mockito.spy(obsObject.getObjectContent());
        obsObject.setObjectContent(mockInputStream);
        Mockito.doReturn(obsObject)
            .when(mockObsClient)
            .getObject(anyObject());

        // 重试成功
        Mockito.doThrow(ioException)
            .doThrow(ioException)
            .doCallRealMethod()
            .when(mockInputStream)
            .read();
        FSDataInputStream inputStream = null;
        try {
            inputStream = mockFs.open(testFile, 4096);
            int read = inputStream.read();
            Assert.assertTrue("testReadWithOneByte", 1 == read);
        } catch (IOException e) {
            Assert.assertTrue("testReadWithOneByte",e.getClass()== ioException.getClass());
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }

        // 重试失败
        setNewRetryParam();
        Mockito.doThrow(ioException)
            .when(mockInputStream)
            .read();
        try {
            inputStream = mockFs.open(testFile, 4096);
            inputStream.read();
        } catch (IOException e) {
            Assert.assertTrue("testReadWithOneByte",e.getClass()== ioException.getClass());
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }


    @Test
    public void testReadByByteBuffer() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile);
        outputStream.write("123456".getBytes());
        outputStream.close();
        ByteBuffer byteBuffer = ByteBuffer.allocate(5);
        //obsclient异常
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;

            try {
                inputStream = mockFs.open(testFile, 4096);
                //EOF异常
                int read = inputStream.read(byteBuffer);
                Assert.assertTrue("testReadWithBytes",read == -1);
            } catch (IOException e) {
                Assert.assertTrue("testReadWithBytes",e.getClass()== noRetryExceptions.get(ex));
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }

        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;
            try {
                inputStream = mockFs.open(testFile, 4096);
                byteBuffer.clear();
                inputStream.read(byteBuffer);
                Assert.assertTrue("testReadWithBytes", "12345".equals(
                    new String(byteBuffer.array()).substring(0,5)));
            } catch (IOException e) {
                Assert.assertTrue("testReadWithBytes",false);
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;
            try {
                inputStream = mockFs.open(testFile, 4096);
                byteBuffer.clear();
                inputStream.read(byteBuffer);
            } catch (IOException e) {
                Assert.assertTrue("testReadWithBytes",e.getClass()== retryExceptions.get(ex));
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }
    }

    @Test
    public void testReadByByteBufferWithIO() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile);
        outputStream.write("123456".getBytes());
        outputStream.close();
        ByteBuffer byteBuffer = ByteBuffer.allocate(5);

        //读IO异常
        IOException ioException = new IOException("mock IOException");
        IOException eofException = new EOFException("mock EOFException");

        ObsObject obsObject = new ObsObject();
        obsObject.setBucketName(fs.getBucket());
        obsObject.setObjectContent(new InputStream() {
            @Override
            public int read(byte b[], int off, int len) throws IOException {
                System.arraycopy("123456".getBytes(),0, b, off, len);
                return len;
            }
            @Override
            public int read() throws IOException {
                return 1;
            }
        });

        InputStream mockInputStream = Mockito.spy(obsObject.getObjectContent());
        obsObject.setObjectContent(mockInputStream);
        Mockito.doReturn(obsObject)
            .when(mockObsClient)
            .getObject(anyObject());

        // 重试成功
        Mockito.doThrow(ioException)
            .doThrow(ioException)
            .doCallRealMethod()
            .when(mockInputStream)
            .read(anyObject(),anyInt(),anyInt());
        FSDataInputStream inputStream = null;
        try {
            inputStream = mockFs.open(testFile, 4096);
            byteBuffer.clear();
            inputStream.read(byteBuffer);
            Assert.assertTrue("testReadWithBytes", "12345".equals(
                new String(byteBuffer.array()).substring(0,5)));
        } catch (IOException e) {
            Assert.assertTrue("testReadWithBytes",e.getClass()== ioException.getClass());
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }

        // 重试失败
        setNewRetryParam();
        Mockito.doThrow(ioException)
            .when(mockInputStream)
            .read(anyObject(),anyInt(),anyInt());
        try {
            inputStream = mockFs.open(testFile, 4096);
            byteBuffer.clear();
            inputStream.read(byteBuffer);
        } catch (IOException e) {
            Assert.assertTrue("testReadWithBytes",e.getClass()== ioException.getClass());
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }


    @Test
    public void testReadByRandom() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile);
        outputStream.write("123456".getBytes());
        outputStream.close();
        byte[] buffer = new byte[6];

        //obsclient异常
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;

            try {
                inputStream = mockFs.open(testFile, 4096);
                //EOF异常
                int read = inputStream.read(1, buffer, 0, 4);
                Assert.assertTrue("testReadByRandom",read == -1);
            } catch (IOException e) {
                Assert.assertTrue("testReadByRandom",e.getClass()== noRetryExceptions.get(ex));
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;
            try {
                inputStream = mockFs.open(testFile, 4096);
                inputStream.read(1, buffer, 0, 5);
                Assert.assertTrue("testReadByRandom", "23456".equals(new String(buffer).substring(0,5)));
            } catch (IOException e) {
                Assert.assertTrue("testReadByRandom",false);
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;
            try {
                inputStream = mockFs.open(testFile, 4096);
                inputStream.read(1, buffer, 0, 5);
            } catch (IOException e) {
                Assert.assertTrue("testReadByRandom",e.getClass()== retryExceptions.get(ex));
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }
    }

    @Test
    public void testReadByRandomWithIO() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile);
        outputStream.write("123456".getBytes());
        outputStream.close();
        byte[] buffer = new byte[6];

        //读IO异常
        IOException ioException = new IOException("mock IOException");
        IOException eofException = new EOFException("mock EOFException");

        ObsObject obsObject = new ObsObject();
        obsObject.setBucketName(fs.getBucket());
        obsObject.setObjectContent(new InputStream() {
            @Override
            public int read(byte b[], int off, int len) throws IOException {
                System.arraycopy("123456".getBytes(),0, b, off, len);
                return len;
            }
            @Override
            public int read() throws IOException {
                return 1;
            }
        });

        InputStream mockInputStream = Mockito.spy(obsObject.getObjectContent());
        obsObject.setObjectContent(mockInputStream);
        Mockito.doReturn(obsObject)
            .when(mockObsClient)
            .getObject(anyObject());

        // 重试成功
        Mockito.doThrow(ioException)
            .doThrow(ioException)
            .doCallRealMethod()
            .when(mockInputStream)
            .read(anyObject(),anyInt(),anyInt());
        FSDataInputStream inputStream = null;
        try {
            inputStream = mockFs.open(testFile, 4096);
            inputStream.read(1, buffer, 0, 5);
            Assert.assertTrue("testReadByRandom", "12345".equals(new String(buffer).substring(0,5)));
        } catch (IOException e) {
            Assert.assertTrue("testReadByRandom",e.getClass()== ioException.getClass());
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }

        // 重试失败
        setNewRetryParam();
        Mockito.doThrow(ioException)
            .when(mockInputStream)
            .read(anyObject(),anyInt(),anyInt());
        try {
            inputStream = mockFs.open(testFile, 4096);
            inputStream.read(1, buffer, 0, 5);
        } catch (IOException e) {
            Assert.assertTrue("testReadByRandom",e.getClass()== ioException.getClass());
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }


    //randomReadWithNewInputStream 重试机制
    @Test
    public void testReadByRandomWithOptimize() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile);
        outputStream.write("123456".getBytes());
        outputStream.close();
        byte[] buffer = new byte[6];

        Mockito.doReturn(false)
            .when(mockFs).isReadTransformEnabled();

        //obsclient异常
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;

            try {
                inputStream = mockFs.open(testFile, 4096);
                //EOF异常
                int read = inputStream.read(1, buffer, 0, 4);
                Assert.assertTrue("testReadByRandomWithOptimize",read == -1);
            } catch (IOException e) {
                Assert.assertTrue("testReadByRandomWithOptimize",e.getClass()== noRetryExceptions.get(ex));
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;
            try {
                inputStream = mockFs.open(testFile, 4096);
                inputStream.read(1, buffer, 0, 5);
                Assert.assertTrue("testReadByRandomWithOptimize", "23456".equals(new String(buffer).substring(0,5)));
            } catch (IOException e) {
                Assert.assertTrue("testReadByRandomWithOptimize",false);
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .getObject(anyObject());
            FSDataInputStream inputStream = null;
            try {
                inputStream = mockFs.open(testFile, 4096);
                inputStream.read(1, buffer, 0, 5);
            } catch (IOException e) {
                Assert.assertTrue("testReadByRandomWithOptimize",e.getClass()== retryExceptions.get(ex));
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }
    }

    //randomReadWithNewInputStream 重试机制
    @Test
    public void testReadByRandomWithOptimizeIO() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile);
        outputStream.write("123456".getBytes());
        outputStream.close();
        byte[] buffer = new byte[6];

        Mockito.doReturn(false)
            .when(mockFs).isReadTransformEnabled();

        //读IO异常
        IOException ioException = new IOException("mock IOException");
        IOException eofException = new EOFException("mock EOFException");

        ObsObject obsObject = new ObsObject();
        obsObject.setBucketName(fs.getBucket());
        obsObject.setObjectContent(new InputStream() {
            @Override
            public int read(byte b[], int off, int len) throws IOException {
                System.arraycopy("123456".getBytes(),0, b, off, len);
                return len;
            }
            @Override
            public int read() throws IOException {
                return 1;
            }
        });

        InputStream mockInputStream = Mockito.spy(obsObject.getObjectContent());
        obsObject.setObjectContent(mockInputStream);
        Mockito.doReturn(obsObject)
            .when(mockObsClient)
            .getObject(anyObject());

        // 重试成功
        Mockito.doThrow(ioException)
            .doThrow(ioException)
            .doCallRealMethod()
            .when(mockInputStream)
            .read(anyObject(),anyInt(),anyInt());
        FSDataInputStream inputStream = null;
        try {
            inputStream = mockFs.open(testFile, 4096);
            inputStream.read(1, buffer, 0, 5);
            Assert.assertTrue("testReadByRandomWithOptimize", "12345".equals(new String(buffer).substring(0,5)));
        } catch (IOException e) {
            Assert.assertTrue("testReadByRandomWithOptimize",e.getClass()== ioException.getClass());
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }


        // 重试失败
        setNewRetryParam();
        Mockito.doThrow(ioException)
            .doThrow(ioException)
            .doThrow(ioException)
            .doCallRealMethod()
            .when(mockInputStream)
            .read(anyObject(),anyInt(),anyInt());

        try {
            inputStream = mockFs.open(testFile, 4096);
            inputStream.read(1, buffer, 0, 5);
        } catch (IOException e) {
            Assert.assertTrue("testReadByRandomWithOptimize",e.getClass()== ioException.getClass());
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    @Test
    public void testDelete() throws Exception {
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .deleteObject(anyObject());
            try {
                OBSCommonUtils.deleteObject(mockFs,
                    testFile.toString().substring(1));
            } catch (IOException e) {
                Assert.assertTrue("testDelete",e.getClass()== noRetryExceptions.get(ex));
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .deleteObject(anyObject());
            try {
                OBSCommonUtils.deleteObject(mockFs,
                    testFile.toString().substring(1));
            } catch (IOException e) {
                Assert.assertTrue("testDelete",false);
            }
        }

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .deleteObject(anyObject());
            try {
                OBSCommonUtils.deleteObject(mockFs,
                    testFile.toString().substring(1));
            } catch (IOException e) {
                Assert.assertTrue("testDelete", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    public void testObjectRenameByCopyObject() throws Exception {
        if (fs.isFsBucket()) {
            return;
        }
        Path srcFile = getTestPath("srcFile");
        Path destFile = getTestPath("destFile");
        FSDataOutputStream os = fs.create(srcFile);
        OBSFSTestUtil.writeData(os, 1024 * 1024);
        os.close();

        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .copyObject(anyObject());
            try {
                OBSObjectBucketUtils.copyFile(mockFs, srcFile.toString().substring(1),
                    destFile.toString().substring(1), 10);
            } catch (IOException e) {
                Assert.assertTrue("testObjectRenameByCopyObject",e.getClass()== noRetryExceptions.get(ex));
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .copyObject(anyObject());
            try {
                OBSObjectBucketUtils.copyFile(mockFs, srcFile.toString().substring(1),
                    destFile.toString().substring(1), 10);
            } catch (IOException e) {
                Assert.assertTrue("testObjectRenameByCopyObject",false);
            }
        }
    }

    @Test
    public void testObjectRenameByCopyObjectFail() throws Exception {
        if (fs.isFsBucket()) {
            return;
        }
        Path srcFile = getTestPath("srcFile");
        Path destFile = getTestPath("destFile");
        FSDataOutputStream os = fs.create(srcFile);
        OBSFSTestUtil.writeData(os, 1024 * 1024);
        os.close();

        //重试失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .copyObject(anyObject());
            try {
                OBSObjectBucketUtils.copyFile(mockFs, srcFile.toString().substring(1),
                    destFile.toString().substring(1), 10);
            } catch (IOException e) {
                Assert.assertTrue("testObjectRenameByCopyObject", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    public void testObjectRenameByCopyPart() throws Exception {
        if (fs.isFsBucket()) {
            return;
        }
        Path srcFile = getTestPath("srcFile");
        Path destFile = getTestPath("destFile");
        FSDataOutputStream os = fs.create(srcFile);
        OBSFSTestUtil.writeData(os, 10 * 1024 * 1024);
        os.close();

        //多段copy阈值设置为5MB
        Mockito.doReturn(5 * 1024 * 1024L)
            .when(mockFs).getCopyPartSize();
        for (Exception ex : noRetryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .copyPart(anyObject());
            try {
                OBSObjectBucketUtils.copyFile(mockFs, srcFile.toString().substring(1),
                    destFile.toString().substring(1), 10 * 1024 * 1024);
            } catch (IOException e) {
                Assert.assertTrue("testObjectRenameByCopyPart",e.getClass()== noRetryExceptions.get(ex));
            }
        }
        // 重试第3次成功
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .doThrow(ex)
                .doThrow(ex)
                .doThrow(ex)
                .doCallRealMethod()
                .when(mockObsClient)
                .copyPart(anyObject());
            try {
                OBSObjectBucketUtils.copyFile(mockFs, srcFile.toString().substring(1),
                    destFile.toString().substring(1), 10 * 1024 * 1024);
            } catch (IOException e) {
                Assert.assertTrue("testObjectRenameByCopyPart",false);
            }
        }
    }

    @Test
    public void testObjectRenameByCopyPartFail() throws Exception {
        if (fs.isFsBucket()) {
            return;
        }
        Path srcFile = getTestPath("srcFile");
        Path destFile = getTestPath("destFile");
        FSDataOutputStream os = fs.create(srcFile);
        OBSFSTestUtil.writeData(os, 10 * 1024 * 1024);
        os.close();

        //多段copy阈值设置为5MB
        Mockito.doReturn(5 * 1024 * 1024L)
            .when(mockFs).getCopyPartSize();

        //重试第3次失败
        setNewRetryParam();
        for (Exception ex : retryExceptions.keySet()) {
            Mockito.doThrow(ex)
                .when(mockObsClient)
                .copyPart(anyObject());
            try {
                OBSObjectBucketUtils.copyFile(mockFs, srcFile.toString().substring(1),
                    destFile.toString().substring(1), 10 * 1024 * 1024);
            } catch (IOException e) {
                Assert.assertTrue("testObjectRenameByCopyPart", e.getClass()== retryExceptions.get(ex));
            }
        }
    }

    @Test
    // 测试文件桶递归删除时，服务端返回409，在OBSA侧执行重试
    public void testFsDeleteWith409Exception() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        // 构造多级目录结构
        Path dir01 = getTestPath("dir01/");
        Path subDir01 = getTestPath("dir01/subDir01/");
        Path subFile01 = getTestPath("dir01/subFile01");
        fs.mkdirs(dir01);
        fs.mkdirs(subDir01);
        FSDataOutputStream os = fs.create(subFile01);
        OBSFSTestUtil.writeData(os, 1024 * 1024);
        os.close();

        boolean hasException = false;
        Mockito.doThrow(exception409)
            .doThrow(exception409)
            .doCallRealMethod()
            .when(mockObsClient)
            .deleteObject(anyString(), anyString());
        Mockito.doThrow(exception409)
            .doThrow(exception409)
            .doCallRealMethod()
            .when(mockObsClient)
            .deleteObjects(anyObject());

        // 409重试后成功
        try {
            mockFs.delete(dir01, true);
        } catch (Exception e) {
            hasException = true;
        }
        assertFalse("delete folder should not has exception", hasException);
    }

    @Test
    // 测试文件桶递归删除时，服务端返回409，在OBSA侧执行重试
    public void testFsDeleteWith409ExceptionFail() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        // 构造多级目录结构
        Path dir01 = getTestPath("dir01/");
        Path subDir01 = getTestPath("dir01/subDir01/");
        Path subFile01 = getTestPath("dir01/subFile01");
        fs.mkdirs(dir01);
        fs.mkdirs(subDir01);
        FSDataOutputStream os = fs.create(subFile01);
        OBSFSTestUtil.writeData(os, 1024 * 1024);
        os.close();

        setNewRetryParam();
        boolean hasException = false;
        // 409重试后不成功
        Mockito.doThrow(exception409)
            .when(mockObsClient)
            .deleteObject(anyString(), anyString());
        Mockito.doThrow(exception409)
            .when(mockObsClient)
            .deleteObjects(anyObject());
        try {
            mockFs.delete(dir01, true);
        } catch (Exception e) {
            hasException = true;
        }
        assertTrue("delete folder has exception", hasException);
    }


    private void setRetryParam() {
        Configuration conf = new Configuration();
        conf.setLong(OBSConstants.RETRY_SLEEP_BASETIME, 5);
        conf.setLong(OBSConstants.RETRY_SLEEP_MAXTIME, 10);
        conf.setLong(OBSConstants.RETRY_QOS_SLEEP_BASETIME, 5);
        conf.setLong(OBSConstants.RETRY_QOS_SLEEP_MAXTIME, 10);
        OBSCommonUtils.init(fs, conf);
    }

    private void setNewRetryParam() {
        Configuration conf = new Configuration();
        conf.setLong(OBSConstants.RETRY_MAXTIME, 10);
        conf.setLong(OBSConstants.RETRY_SLEEP_BASETIME, 5);
        conf.setLong(OBSConstants.RETRY_SLEEP_MAXTIME, 10);
        conf.setLong(OBSConstants.RETRY_QOS_MAXTIME, 10);
        conf.setLong(OBSConstants.RETRY_QOS_SLEEP_BASETIME, 5);
        conf.setLong(OBSConstants.RETRY_QOS_SLEEP_MAXTIME, 10);
        OBSCommonUtils.init(fs, conf);
    }
}
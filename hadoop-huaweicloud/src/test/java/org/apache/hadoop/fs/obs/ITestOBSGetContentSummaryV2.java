package org.apache.hadoop.fs.obs;

import static org.apache.hadoop.fs.obs.OBSTestConstants.TEST_FS_OBS_NAME;

import com.obs.services.IObsCredentialsProvider;
import com.obs.services.ObsConfiguration;
import com.obs.services.exception.ObsException;
import com.obs.services.model.fs.ContentSummaryFsRequest;
import com.obs.services.model.fs.ContentSummaryFsResult;
import com.obs.services.model.fs.DirContentSummary;
import com.obs.services.model.fs.DirSummary;
import com.obs.services.model.fs.ListContentSummaryFsRequest;
import com.obs.services.model.fs.ListContentSummaryFsResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.fs.obs.mock.MockObsClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class ITestOBSGetContentSummaryV2 {

    private static boolean failure;

    private static final Logger LOG = LoggerFactory.getLogger(ITestOBSGetContentSummaryV2.class);

    private OBSFileSystem fs;

    private Configuration conf;

    private MockObsClient mockObsClient;

    private OBSFileSystem ofs;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private boolean useMock = true;

    @Before
    public void setUp() throws IOException, URISyntaxException, NoSuchFieldException, IllegalAccessException {
        if (failure) return;
        conf = OBSContract.getConfiguration(null);
        conf.setInt(OBSConstants.CORE_LIST_THREADS, 100);
        conf.setInt(OBSConstants.MAX_LIST_THREADS, 100);
        conf.setInt(OBSConstants.RETRY_MAXTIME, 5000);
        fs = OBSTestUtils.createTestFileSystem(conf);
        ofs = OBSTestUtils.createTestFileSystem(conf);
        if (useMock) {
            mock();
        }
    }

    private void mock() throws NoSuchFieldException, IOException, IllegalAccessException, URISyntaxException {
        // create mock client
        String fsname = conf.getTrimmed(TEST_FS_OBS_NAME, "");
        URI uri = new URI(fsname);
        ObsConfiguration obsConf = new ObsConfiguration();
        String endPoint = conf.getTrimmed(OBSConstants.ENDPOINT, "");
        obsConf.setEndPoint(endPoint);
        IObsCredentialsProvider securityProvider = OBSSecurityProviderUtil.createObsSecurityProvider(conf, uri);
        mockObsClient = new MockObsClient(securityProvider, obsConf);

        // mock
        Field obsFiled = OBSFileSystem.class.getDeclaredField("obs");
        obsFiled.setAccessible(true);
        obsFiled.set(this.fs, mockObsClient);
    }

    @After
    public void cleanUp() throws IOException {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }

    private void createFile(Path path, long fileSize) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        for (long i = 0; i < fileSize; i ++) {
            stringBuilder.append((char)1);
        }
        FSDataOutputStream outputStream = fs.create(path);
        outputStream.write(stringBuilder.toString().getBytes());
        outputStream.close();
    }

    private void createFiles(String basePath, int fileNum, int parDirLevel, long fileSize) throws IOException {
        if (parDirLevel < 1) {
            throw new IllegalArgumentException("parDirLevel should >= 1");
        }
        for (int i = 0; i < fileNum; i ++) {
            String path = String.format("%s/sub-dir%d", basePath, i);
            for (int j = 0; j < parDirLevel - 1; j ++) {
                path = String.format("%s/sub-dir-level%d", path, j);
            }
            path = path + "/file";
            createFile(new Path(path), fileSize);
        }
    }

    @Test
    public void testFsContentSummaryV2_1() throws IOException {
        fs.mkdirs(new Path(testRootPath));
        FileStatus fileStatus = fs.getFileStatus(new Path(testRootPath));
        if (!useMock) {
            try {
                Thread.sleep(30 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        ContentSummary contentSummary = OBSPosixBucketUtils.fsGetDirectoryContentSummaryV2(fs, fileStatus);
        assertTrue("contentSummary correctness",
            contentSummary.getDirectoryCount() == 0 + 1 &&
            contentSummary.getFileCount() == 0 &&
            contentSummary.getLength() == 0);
    }

    @Test
    public void testFsContentSummaryV2_2() throws IOException {
        Path path = new Path(testRootPath + "/dir1");
        fs.mkdirs(path);
        FileStatus fileStatus = fs.getFileStatus(path);
        if (!useMock) {
            try {
                Thread.sleep(30 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        ContentSummary contentSummary = OBSPosixBucketUtils.fsGetDirectoryContentSummaryV2(fs, fileStatus);
        assertTrue("contentSummary correctness",
            contentSummary.getDirectoryCount() == 0 + 1 &&
                contentSummary.getFileCount() == 0 &&
                contentSummary.getLength() == 0);
    }

    @Test
    public void testFsContentSummaryV2_3() throws IOException {
        Path path = new Path(testRootPath + "/dir1/dir2");
        fs.mkdirs(path);
        FileStatus fileStatus = fs.getFileStatus(new Path(testRootPath + "/dir1"));
        if (!useMock) {
            try {
                Thread.sleep(30 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        ContentSummary contentSummary = OBSPosixBucketUtils.fsGetDirectoryContentSummaryV2(fs, fileStatus);
        assertTrue("contentSummary correctness",
            contentSummary.getDirectoryCount() == 1 + 1 &&
                contentSummary.getFileCount() == 0 &&
                contentSummary.getLength() == 0);
    }

    @Test
    public void testFsContentSummaryV2_4() throws IOException {
        Path path = new Path(testRootPath + "/dir1/dir2");
        fs.mkdirs(path);
        FileStatus fileStatus = fs.getFileStatus(path);
        if (!useMock) {
            try {
                Thread.sleep(30 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        ContentSummary contentSummary = OBSPosixBucketUtils.fsGetDirectoryContentSummaryV2(fs, fileStatus);
        assertTrue("contentSummary correctness",
            contentSummary.getDirectoryCount() == 0 + 1 &&
                contentSummary.getFileCount() == 0 &&
                contentSummary.getLength() == 0);
    }

    @Test
    public void testFsContentSummaryV2_5() throws IOException {
        Path path = new Path(testRootPath + "/dir1/dir2/file1");
        createFile(path, 1);
        FileStatus fileStatus = fs.getFileStatus(new Path(testRootPath + "/dir1"));
        if (!useMock) {
            try {
                Thread.sleep(30 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        ContentSummary contentSummary = OBSPosixBucketUtils.fsGetDirectoryContentSummaryV2(fs, fileStatus);
        assertTrue("contentSummary correctness",
            contentSummary.getDirectoryCount() == 1 + 1 &&
                contentSummary.getFileCount() == 1 &&
                contentSummary.getLength() == 1);
    }

    @Test
    public void testFsContentSummaryV2_6() throws IOException {
        Path path = new Path(testRootPath + "/dir1/dir2/file1");
        createFile(path, 1);
        FileStatus fileStatus = fs.getFileStatus(new Path(testRootPath + "/dir1/dir2"));
        if (!useMock) {
            try {
                Thread.sleep(30 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        ContentSummary contentSummary = OBSPosixBucketUtils.fsGetDirectoryContentSummaryV2(fs, fileStatus);
        assertTrue("contentSummary correctness",
            contentSummary.getDirectoryCount() == 0 + 1 &&
                contentSummary.getFileCount() == 1 &&
                contentSummary.getLength() == 1);
    }

    @Test
    public void testFsContentSummaryV2_7() throws IOException {
        createFile(new Path(testRootPath + "/dir1/file1"), 1);
        createFile(new Path(testRootPath + "/dir1/dir2/file2"), 2);
        FileStatus fileStatus = fs.getFileStatus(new Path(testRootPath + "/dir1"));
        if (!useMock) {
            try {
                Thread.sleep(30 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        ContentSummary contentSummary = OBSPosixBucketUtils.fsGetDirectoryContentSummaryV2(fs, fileStatus);
        assertTrue("contentSummary correctness",
            contentSummary.getDirectoryCount() == 1 + 1 &&
                contentSummary.getFileCount() == 2 &&
                contentSummary.getLength() == 3);
    }

    private void testFsContentSummaryV2MultiDirsAndFiles(int fileNum, int parDirLevel,
        Function<ListContentSummaryFsResult, ListContentSummaryFsResult> listCSResultCallback) throws IOException {
        long fileSize = 3;
        createFiles(testRootPath + "/dir1", fileNum, parDirLevel, fileSize);
        FileStatus fileStatus = fs.getFileStatus(new Path(testRootPath + "/dir1"));
        if (!useMock) {
            try {
                Thread.sleep(300 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        this.mockObsClient.setListCSResultCallback(listCSResultCallback);
        ContentSummary contentSummary = OBSPosixBucketUtils.fsGetDirectoryContentSummaryV2(fs, fileStatus);
        LOG.info("V2: dirNum: {}, fileNum: {}, fileSize: {}, should be: ({}, {}, {})",
            contentSummary.getDirectoryCount(), contentSummary.getFileCount(), contentSummary.getLength(),
            fileNum * parDirLevel + 1, fileNum, fileNum * fileSize);
        boolean pass = contentSummary.getDirectoryCount() == (long) fileNum * parDirLevel + 1 &&
            contentSummary.getFileCount() == fileNum &&
            contentSummary.getLength() == fileNum * fileSize;
        if (!pass) {
            failure = true;
        }

        assertTrue("contentSummary correctness", pass);

        // compare with v1
        ContentSummary contentSummary1 = OBSPosixBucketUtils.fsGetDirectoryContentSummary(fs,
            OBSCommonUtils.pathToKey(fs, fileStatus.getPath()));
        LOG.info("V1: dirNum: {}, fileNum: {}, fileSize: {}",
            contentSummary1.getDirectoryCount(), contentSummary1.getFileCount(), contentSummary1.getLength());
        assertTrue("v2 should align to v1",
            contentSummary.getDirectoryCount() == contentSummary1.getDirectoryCount() &&
                contentSummary.getFileCount() == contentSummary1.getFileCount() &&
                contentSummary.getLength() == contentSummary1.getLength());
    }

    @Test
    public void testFsContentSummaryV2_8() throws IOException {
        testFsContentSummaryV2MultiDirsAndFiles(1100, 1, null);
    }

    @Test
    public void testFsContentSummaryV2_9() throws IOException {
        testFsContentSummaryV2MultiDirsAndFiles(900, 1, null);
    }

    @Test
    public void testFsContentSummaryV2_10() throws IOException {
        testFsContentSummaryV2MultiDirsAndFiles(1000, 1, null);
    }

    @Test
    public void testFsContentSummaryV2_11() throws IOException {
        testFsContentSummaryV2MultiDirsAndFiles(1100, 2, null);
    }

    @Test
    public void testFsContentSummaryV2_12() throws IOException {
        testFsContentSummaryV2MultiDirsAndFiles(900, 2, null);
    }

    @Test
    public void testFsContentSummaryV2_13() throws IOException {
        testFsContentSummaryV2MultiDirsAndFiles(1000, 2, null);
    }

    @Test
    public void testFsContentSummaryV2_14() throws IOException {
        Path root = new Path("/");
        FileStatus fileStatus = fs.getFileStatus(root);
        try {
            OBSPosixBucketUtils.fsGetDirectoryContentSummaryV2(fs, fileStatus);
        } catch (Exception e) {
            assertTrue(e.getMessage(), false);
        }
        assertTrue(true);
    }

    private static class ErrorPattern1 {
        private volatile AtomicInteger count;

        private int[] errorPoint;

        private int errorDirNum;

        public ErrorPattern1(int[] errorPoint, int errorDirNum) {
            this.errorPoint = errorPoint;
            this.errorDirNum = errorDirNum;
            this.count = new AtomicInteger();
        }

        public ListContentSummaryFsResult callbackFunc(ListContentSummaryFsResult listCSResult) {
            this.count.incrementAndGet();
            boolean needInject = false;
            for (int i : errorPoint) {
                if (this.count.get() == i) {
                    needInject = true;
                    break;
                }
            }
            if (!needInject) {
                return listCSResult;
            }
            List<ListContentSummaryFsResult.ErrorResult> retErrors = new ArrayList<>();
            List<DirContentSummary> retDirs = new ArrayList<>();
            List<DirContentSummary> dirs = listCSResult.getDirContentSummaries();
            int nNum = Math.min(dirs.size(), errorDirNum);
            for (int i = 0; i < dirs.size() - nNum; i ++) {
                retDirs.add(dirs.get(i));
            }
            for (int i = dirs.size() - nNum; i < dirs.size(); i ++) {
                DirContentSummary dir = dirs.get(i);
                ListContentSummaryFsResult.ErrorResult err = new ListContentSummaryFsResult.ErrorResult();
                err.setStatusCode("500");
                err.setErrorCode("TestErrorCode");
                err.setKey(dir.getKey());
                err.setInode(dir.getInode());
                err.setMessage("InjectError");
                retErrors.add(err);
            }
            listCSResult.setDirContentSummaries(retDirs);
            listCSResult.setErrorResults(retErrors);
            return listCSResult;
        }
    }

    @Test
    public void testFsContentSummaryV2_15() throws IOException {
        if (!useMock) {
            return;
        }
        ErrorPattern1 ep = new ErrorPattern1(new int[]{1, 3, 5}, 3);
        testFsContentSummaryV2MultiDirsAndFiles(1100, 1,
            ep::callbackFunc);
    }

    @Test
    public void testFsContentSummaryV2_16() throws IOException {
        if (!useMock) {
            return;
        }
        ErrorPattern1 ep = new ErrorPattern1(new int[]{1, 3, 5}, 3);
        testFsContentSummaryV2MultiDirsAndFiles(900, 1,
            ep::callbackFunc);
    }

    @Test
    public void testFsContentSummaryV2_17() throws IOException {
        if (!useMock) {
            return;
        }
        ErrorPattern1 ep = new ErrorPattern1(new int[]{1, 3, 5}, 3);
        testFsContentSummaryV2MultiDirsAndFiles(1000, 1,
            ep::callbackFunc);
    }

    @Test
    public void testFsContentSummaryV2_18() throws IOException {
        if (!useMock) {
            return;
        }
        ErrorPattern1 ep = new ErrorPattern1(new int[]{1, 3, 5}, 3);
        testFsContentSummaryV2MultiDirsAndFiles(1100, 2,
            ep::callbackFunc);
    }

    @Test
    public void testFsContentSummaryV2_19() throws IOException {
        if (!useMock) {
            return;
        }
        ErrorPattern1 ep = new ErrorPattern1(new int[]{1, 3, 5}, 3);
        testFsContentSummaryV2MultiDirsAndFiles(900, 2,
            ep::callbackFunc);
    }

    @Test
    public void testFsContentSummaryV2_20() throws IOException {
        if (!useMock) {
            return;
        }
        ErrorPattern1 ep = new ErrorPattern1(new int[]{1, 3, 5}, 3);
        testFsContentSummaryV2MultiDirsAndFiles(1000, 2,
            ep::callbackFunc);
    }

    @Test
    public void test405FallbackWithoutRetry() throws IOException {
        if (useMock) {
            mockObsClient.setGetCSUnsupported(true);
            mockObsClient.setGetCSNum(0);
            mockObsClient.setResponseCode(405);
            mockObsClient.setErrorMsg("mock unsupported");
            createFile(new Path(testRootPath + "/dir1/file1"), 1);
            createFile(new Path(testRootPath + "/dir1/dir2/file2"), 2);
            Path path = new Path(testRootPath + "/dir1");
            ContentSummary contentSummary = fs.getContentSummary(path);
            assertTrue("contentSummary correctness",
                contentSummary.getDirectoryCount() == 1 + 1 &&
                    contentSummary.getFileCount() == 2 &&
                    contentSummary.getLength() == 3);
            assertEquals("escape without retry", 1, mockObsClient.getGetCSNum());
            mockObsClient.setGetCSUnsupported(false);
            mockObsClient.setGetCSNum(0);
        }
    }

    @Test
    public void test503FallbackShouldRetry() throws IOException {
        if (useMock) {
            mockObsClient.setGetCSUnsupported(true);
            mockObsClient.setGetCSNum(0);
            mockObsClient.setResponseCode(503);
            mockObsClient.setErrorMsg("mock service unavailable");
            createFile(new Path(testRootPath + "/dir1/file1"), 1);
            createFile(new Path(testRootPath + "/dir1/dir2/file2"), 2);
            Path path = new Path(testRootPath + "/dir1");
            ContentSummary contentSummary = fs.getContentSummary(path);
            assertTrue("contentSummary correctness",
                contentSummary.getDirectoryCount() == 1 + 1 &&
                    contentSummary.getFileCount() == 2 &&
                    contentSummary.getLength() == 3);
            assertTrue("escape with retry", mockObsClient.getGetCSNum() > 1);
            mockObsClient.setGetCSUnsupported(false);
            mockObsClient.setGetCSNum(0);
        }
    }

    // @Test
    public void testFsContentSummaryV2() throws IOException {
        FileStatus fileStatus = ofs.getFileStatus(new Path("/"));
        ContentSummary contentSummary = OBSPosixBucketUtils.fsGetDirectoryContentSummaryV2(ofs, fileStatus);
        long dirNum = contentSummary.getDirectoryCount();
        long fileNum = contentSummary.getFileCount();
        long fileSize = contentSummary.getLength();
        System.out.printf("%d %d %d", dirNum, fileNum, fileSize);
    }

    // @Test
    public void testSDK1() {
        List<ListContentSummaryFsRequest.DirLayer> dirs = new ArrayList<>();
        ListContentSummaryFsRequest.DirLayer dir = new ListContentSummaryFsRequest.DirLayer();
        dir.setKey("test/dir1/");
        // dir.setInode(4611752345613959168L);
        dirs.add(dir);

        ListContentSummaryFsRequest req = new ListContentSummaryFsRequest();
        req.setBucketName("sanfangliantiao-0218");
        req.setMaxKeys(1000);
        req.setDirLayers(dirs);

        ListContentSummaryFsResult res = ofs.getObsClient().listContentSummaryFs(req);
        System.out.println(res);
    }

    // @Test
    public void testSDK2() {
        ContentSummaryFsRequest req = new ContentSummaryFsRequest();
        req.setBucketName("jianantest02");
        req.setDirName("test/dir1/sub-dir908/");

        ContentSummaryFsResult res = ofs.getObsClient().getContentSummaryFs(req);
        DirSummary summary = res.getContentSummary();
        System.out.printf("%d %d %d", summary.getDirCount(), summary.getFileCount(), summary.getFileSize());
    }
}

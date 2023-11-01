package org.apache.hadoop.fs.obs;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertEquals;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createAndVerifyFile;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;

import com.obs.services.ObsClient;
import com.obs.services.model.AbortMultipartUploadRequest;
import com.obs.services.model.ListMultipartUploadsRequest;
import com.obs.services.model.MultipartUpload;
import com.obs.services.model.MultipartUploadListing;
import com.obs.services.model.fs.ObsFSAttribute;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;

@RunWith(Parameterized.class)
public class ITestOBSDiskBufferOutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(ITestOBSDiskBufferOutputStream.class);
    private OBSFileSystem fs;
    private boolean calcMd5;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    @Parameterized.Parameters
    public static Collection calcMd5() {
        return Arrays.asList(false, true);
    }

    public ITestOBSDiskBufferOutputStream(boolean calcMd5) {
        this.calcMd5 = calcMd5;
    }

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        conf.setBoolean(OBSConstants.FAST_UPLOAD, true);
        conf.setLong(OBSConstants.MULTIPART_SIZE, 5 * 1024 * 1024);
        conf.setBoolean(OBSConstants.OUTPUT_STREAM_ATTACH_MD5, calcMd5);
        fs = OBSTestUtils.createTestFileSystem(conf);
        deleteTmpDir(conf);
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }

    private Path getTestPath(String relativePath) {
        return new Path(testRootPath + "/" + relativePath);
    }

    @Test
    // disk写缓存，上传对象时构造mock异常，触发异常后，清理上传的碎片
    public void testMockUploadPartError() throws Exception {
        Path dest = getTestPath("testMockUploadPartError");
        IOException exception = null;
        IOException closeException = null;
        FSDataOutputStream stream = null;
        try {
            stream = fs.create(dest, true);
            ((OBSBlockOutputStream) stream.getWrappedStream()).mockPutPartError(
                true);
            byte[] data = ContractTestUtils.dataset(10 * 1204 * 1024, 'a', 26);
            stream.write(data);
            stream.close();
            stream.write(data);
        } catch (IOException e) {
            exception = e;
        } finally {
            try {
                if (stream != null) {
                    stream.close();
                }
            } catch (IOException e) {
                closeException = e;
            }
            fs.delete(dest, false);
        }
        assertTrue(exception != null && exception.getMessage()
            .contains("Multi-part upload"));
        assertTrue(closeException != null && closeException.getMessage()
            .contains("Multi-part upload"));

        // 删除多段碎片
        final Date purgeBefore = new Date();
        ListMultipartUploadsRequest request
            = new ListMultipartUploadsRequest(fs.getBucket());
        while (true) {
            // List + purge
            MultipartUploadListing uploadListing = fs.getObsClient()
                .listMultipartUploads(request);
            for (MultipartUpload upload : uploadListing.getMultipartTaskList()) {
                LOG.info("MultipartTask:create time {},purge time {}",upload.getInitiatedDate(),purgeBefore);
                if (upload.getInitiatedDate().compareTo(purgeBefore) < 0) {
                    LOG.info("abort MultipartTask");
                    fs.getObsClient().abortMultipartUpload(
                        new AbortMultipartUploadRequest(fs.getBucket(), upload.getObjectKey(), upload.getUploadId()));
                }
            }
            if (!uploadListing.isTruncated()) {
                break;
            }
            request.setUploadIdMarker(
                uploadListing.getNextUploadIdMarker());
            request.setKeyMarker(uploadListing.getNextKeyMarker());
        }

        // 校验缓存目录为空
        assertTrue(verifyTmpDirEmpty(fs.getConf()));
    }

    @Test
    // disk写缓存，上传对象并校验数据通过
    public void testBlockUpload() throws IOException {
        //        ContractTestUtils.createAndVerifyFile(fs, testPath, 0);
        //        verifyUpload(100*1024-1);
        verifyUpload(10 * 1024 * 1024 + 1);
        // 校验缓存目录为空
        assertTrue(verifyTmpDirEmpty(fs.getConf()));
        //        verifyUpload(10*1024*1024);
        //        verifyUpload(10*1024*1024-1);
    }

    @Test
    // disk写缓存，测试0字节大小文件上传并校验文件大小
    public void testDiskZeroUpload() throws IOException {
        //        ContractTestUtils.createAndVerifyFile(fs, testPath, 0);
        //        verifyUpload(100*1024-1);
        verifyUpload(0);
        // 校验缓存目录为空
        assertTrue(verifyTmpDirEmpty(fs.getConf()));
    }

    @Test
    // disk写缓存，上传对象关闭流后，再写数据抛出IOException
    public void testDiskWriteAfterStreamClose() throws IOException {
        Path dest = getTestPath("testWriteAfterStreamClose");
        FSDataOutputStream stream = fs.create(dest, true);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        try {
            stream.write(data);
            stream.close();
            boolean hasException = false;
            try {
                stream.write(data);
            } catch (IOException e) {
                hasException = true;
            }
            assertTrue(hasException);

        } finally {
            fs.delete(dest, false);
            IOUtils.closeStream(stream);
        }
        // 校验缓存目录为空
        assertTrue(verifyTmpDirEmpty(fs.getConf()));
    }

    @Test
    // disk写缓存，上传对象流关闭后缓存块被清空
    public void testBlocksClosed() throws Throwable {
        Path dest = getTestPath("testBlocksClosed");

        FSDataOutputStream stream = fs.create(dest, true);
        OBSBlockOutputStream obsStream =
            (OBSBlockOutputStream)stream.getWrappedStream();
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();
        assertTrue(null == obsStream.getActiveBlock());
        fs.delete(dest, false);
        // 校验缓存目录为空
        assertTrue(verifyTmpDirEmpty(fs.getConf()));
    }

    private void verifyUpload(long fileSize) throws IOException {
        createAndVerifyFile(fs, getTestPath("test_file"), fileSize);
    }

    @Test
    // append写，当head返回文件长度与客户端记录长度不一致时，以大的长度为准
    public void testAppendWithIncorrectContentLen() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        OBSFileSystem mockFs = Mockito.spy(fs);
        ObsClient client = fs.getObsClient();
        ObsClient mockClient = Mockito.spy(client);
        Whitebox.setInternalState(mockFs, client, mockClient);

        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile, false);
        byte[] data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        outputStream.write(data);
        outputStream.close();

        // 第一次append调用实际head接口，第二次append使用mock的ObsFSAttribute
        ObsFSAttribute attribute = new ObsFSAttribute();
        attribute.setContentLength(5L);
        Mockito.doCallRealMethod()
            .doReturn(attribute)
            .when(mockClient)
            .getAttribute(anyObject());

        // 第一次写正常，写完后文件长度为20
        outputStream = mockFs.append(testFile, 4096, null);
        outputStream.write(data);
        outputStream.hflush();
        assertEquals(20, fs.getFileStatus(testFile).getLen());

        // 第二次getAttribute得到的文件大小不正确，以大的为准，从20追加写，写完后文件长度为30
        outputStream.write(data);
        outputStream.close();
        assertEquals(30, fs.getFileStatus(testFile).getLen());

        fs.delete(testFile, false);
        // 校验缓存目录为空
        assertTrue(verifyTmpDirEmpty(fs.getConf()));
    }

    private boolean verifyTmpDirEmpty(Configuration conf) throws IOException {
        String bufferDir = conf.get(OBSConstants.BUFFER_DIR) != null
            ? OBSConstants.BUFFER_DIR
            : "hadoop.tmp.dir";
        LocalDirAllocator allocator = new LocalDirAllocator(bufferDir);
        String tmpBuff = "obs-block-0001";
        Path path = allocator.getLocalPathForWrite(tmpBuff, conf);
        File parentDir = new File(path.getParent().toUri().toString());
        assertTrue(parentDir.isDirectory());
        return parentDir.list().length == 0;
    }

    private void deleteTmpDir(Configuration conf) throws IOException {
        String bufferDir = conf.get(OBSConstants.BUFFER_DIR) != null
            ? OBSConstants.BUFFER_DIR
            : "hadoop.tmp.dir";
        LocalDirAllocator allocator = new LocalDirAllocator(bufferDir);
        String tmpBuff = "obs-block-0001";
        Path path = allocator.getLocalPathForWrite(tmpBuff, conf);
        File parentDir = new File(path.getParent().toUri().toString());

        File[]  children = parentDir.listFiles();
        for (int i = 0; i < children.length; i++) {
            children[i].delete();
        }
    }
}
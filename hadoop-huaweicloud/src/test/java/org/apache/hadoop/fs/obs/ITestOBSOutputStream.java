package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class ITestOBSOutputStream {
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
        conf.setLong(OBSConstants.MULTIPART_SIZE, 5 * 1024 * 1024);
        conf.setBoolean(OBSConstants.FAST_UPLOAD, false);
        fs = OBSTestUtils.createTestFileSystem(conf);

    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }

    /**
     * @throws IOException
     */
    @Test
    // 分别上传小于、等于、大于MULTIPART_SIZE大小文件，校验数据正确性
    public void testUpload() throws IOException {
        verifyUpload(5 * 1024 * 1024 + 1);
        verifyUpload(5 * 1024 * 1024);
        verifyUpload(5 * 1024 * 1024 - 1);
    }

    /**
     * @throws IOException
     */
    @Test
    // 上传0字节大小文件，校验文件长度
    public void testZeroUpload() throws IOException {
        verifyUpload(0);
    }

    @Test
    // 文件输出流关闭后，继续写数据，抛出IOException
    public void testWriteAfterStreamClose() throws IOException {
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
            IOUtils.closeStream(stream);
        }
    }

    private Path getTestPath(String relativePath) {
        return new Path(testRootPath + "/" + relativePath);
    }

    private void verifyUpload(long fileSize) throws IOException {
        ContractTestUtils.createAndVerifyFile(fs, getTestPath("test_file"),
            fileSize);
    }
}
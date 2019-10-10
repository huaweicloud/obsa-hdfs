package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertEquals;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createAndVerifyFile;

public class ITestOBSBlockOutputStream{
    private OBSFileSystem fs;
    private static String testRootPath =
            OBSTestUtils.generateUniqueTestPath();
    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean(Constants.FAST_UPLOAD, true);
        fs = OBSTestUtils.createTestFileSystem(conf);


    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }
    private Path getTestPath() {
        return new Path(testRootPath + "/test-obs");
    }
    /**
     *
     * @throws IOException
     */
    @Test
    public void testBlockUpload() throws IOException {
//        ContractTestUtils.createAndVerifyFile(fs, testPath, 0);
//        verifyUpload(100*1024-1);
        verifyUpload(2*1024*1024*1024L+1);
//        verifyUpload(10*1024*1024);
//        verifyUpload(10*1024*1024-1);
    }
    /**
     *
     * @throws IOException
     */
    @Test
    public void testZeroUpload() throws IOException {
//        ContractTestUtils.createAndVerifyFile(fs, testPath, 0);
//        verifyUpload(100*1024-1);
        verifyUpload(0);
    }
    @Test(expected = IOException.class)
    public void testWriteAfterStreamClose() throws Throwable {
        Path dest = new Path(getTestPath(), "testWriteAfterStreamClose");
        FSDataOutputStream stream = fs.create(dest, true);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        try {
            stream.write(data);
            stream.close();
            stream.write(data);

        } finally {
            fs.delete(dest, false);
            IOUtils.closeStream(stream);
        }
    }
    @Test
    public void testBlocksClosed() throws Throwable {
        Path dest = new Path(getTestPath(), "testBlocksClosed");

        FSDataOutputStream stream = fs.create(dest, true);

        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();
        fs.delete(dest, false);
    }
    private void verifyUpload(long fileSize) throws IOException {
        createAndVerifyFile(fs, getTestPath(), fileSize);
    }

}
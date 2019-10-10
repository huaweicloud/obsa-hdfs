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

public class ITestOBSOutputStream{
    private OBSFileSystem fs;
    private static String testRootPath =
            OBSTestUtils.generateUniqueTestPath();
    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean(Constants.FAST_UPLOAD, false);
        fs = OBSTestUtils.createTestFileSystem(conf);


    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }

    /**
     *
     * @throws IOException
     */
    @Test
    public void testUpload() throws IOException {
        verifyUpload(1*1024*1024+1);
        verifyUpload(1*1024*1024);
        verifyUpload(1*1024*1024-1);
    }
    /**
     *
     * @throws IOException
     */
    @Test
    public void testZeroUpload() throws IOException {
        verifyUpload(0);
    }
    @Test(expected = IOException.class)
    public void testWriteAfterStreamClose() throws Throwable {
        Path dest = new Path("testWriteAfterStreamClose");
        FSDataOutputStream stream = fs.create(dest, true);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        try {
            stream.write(data);
            stream.close();
            stream.write(data);

        } finally {
            IOUtils.closeStream(stream);
        }
    }
    private Path getTestPath() {
        return new Path(testRootPath + "/test-obs");
    }
    private void verifyUpload(long fileSize) throws IOException {
        ContractTestUtils.createAndVerifyFile(fs, getTestPath(), fileSize);

    }

}
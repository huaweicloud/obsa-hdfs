package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ITestOBSCopyFromLocalFile {
    private OBSFileSystem fs;
    private static String testRootPath =
            OBSTestUtils.generateUniqueTestPath();
    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        fs = OBSTestUtils.createTestFileSystem(conf);


    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
//            fs.delete(new Path(testRootPath), true);
        }
    }

    @Test
    public void testDirectoryCopyFromLocal() throws IOException {
        Path localPath = new Path("E:\\logs");
        Path dstPath = new Path(testRootPath + "/copylocal-directory/");

        fs.copyFromLocalFile(false,true, localPath, dstPath);
    }

    @Test
    public void testHugeFileCopyFromLocal() throws IOException {
        Path localPath = new Path("D:\\debug\\dummy");
        Path dstPath = new Path(testRootPath + "/copylocal-huge");

        fs.copyFromLocalFile(false,true, localPath, dstPath);
    }
    @Test
    public void testNomalFileCopyFromLocal() throws IOException {
        Path localPath = new Path("D:\\debug\\nomal");
        Path dstPath = new Path(testRootPath + "/copylocal-nomal");

        fs.copyFromLocalFile(false,true, localPath, dstPath);
    }
    @Test
    public void testSmallFileCopyFromLocal() throws IOException {
        Path localPath = new Path("D:\\debug\\small");
        Path dstPath = new Path(testRootPath + "/copylocal-small");

        fs.copyFromLocalFile(false,true, localPath, dstPath);
    }
    @Test
    public void testTinyFileCopyFromLocal() throws IOException {
        Path localPath = new Path("D:\\debug\\tiny");
        Path dstPath = new Path(testRootPath + "/copylocal-tiny");

        fs.copyFromLocalFile(false,true, localPath, dstPath);
    }


}

package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.security.AccessControlException;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ITestOBSListFiles {
    private OBSFileSystem fs;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private static final String SUB_DIR_PREFIX = "sub_dir-";

    private static final String SUB_DIR_FILE_SURFIX = "sub_file-";

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        conf.set(OBSConstants.MULTIPART_SIZE,
            String.valueOf(5 * 1024 * 1024));
        conf.setClass(OBSConstants.OBS_METRICS_CONSUMER,
            MockMetricsConsumer.class, BasicMetricsConsumer.class);
        conf.setBoolean(OBSConstants.METRICS_SWITCH, true);
        fs = OBSTestUtils.createTestFileSystem(conf);
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
    // 路径是根目录，结果数组大小大于等于0
    public void testListFiles001() throws Exception {
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"),
            false);
        long entries = 0;
        while (iterator.hasNext()) {
            entries++;
            iterator.next();
        }
        assertTrue(entries >= 0);
    }

    @Test
    // 路径是一个文件，recursive为true或false，结果数组大小都为1
    public void testListFiles002() throws Exception {
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile, false);
        outputStream.close();

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(testFile,
            false);
        long count = 0;
        while (iterator.hasNext()) {
            count++;
            LocatedFileStatus status = iterator.next();
            assertTrue(status.isFile());
        }
        assertEquals(1, count);

        iterator = fs.listFiles(testFile, true);

        count = 0;
        while (iterator.hasNext()) {
            count++;
            LocatedFileStatus status = iterator.next();
            assertTrue(status.isFile());
        }
        assertEquals(1, count);

        fs.delete(testFile, false);
    }

    @Test
    // 路径是一个目录，每级目录下都有多个目录和文件，recursive为false
    public void testListFiles003() throws Exception {
        Path testPath = getTestPath("test_folder");
        fs.mkdirs(testPath);

        StringBuilder sb = new StringBuilder("test_folder");
        for (int i = 1; i <= 2; i++) {
            sb.append("/");
            sb.append(SUB_DIR_PREFIX + i);
        }
        String lastDir = sb.toString();

        Path lastDirPath = getTestPath(lastDir);
        Path parent = lastDirPath.getParent();
        System.out.println("a     " + parent);

        while (!parent.equals(testPath)) {
            addSubDirUnderSpecificDir(parent, 2);
            addFileUnderSpecificDir(parent, 3);
            parent = parent.getParent();
            System.out.println("b     " + parent);
        }

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(testPath,
            false);

        long count = 0;
        while (iterator.hasNext()) {
            count++;
            LocatedFileStatus status = iterator.next();
            System.out.println(status.toString());
            System.out.println("count: " + count);
        }

        //        assertEquals(1, count);
    }

    @Test
    // 路径是一个目录，每级目录下都有多个目录和文件，recursive为true
    public void testListFiles004() throws Exception {
        Path testPath = getTestPath("test_folder");
        fs.mkdirs(testPath);

        StringBuilder sb = new StringBuilder("test_folder");
        for (int i = 1; i <= 2; i++) {
            sb.append("/");
            sb.append(SUB_DIR_PREFIX + i);
        }
        String lastDir = sb.toString();

        Path lastDirPath = getTestPath(lastDir);
        Path parent = lastDirPath.getParent();
        System.out.println("a     " + parent);

        while (!parent.equals(testPath)) {
            addSubDirUnderSpecificDir(parent, 2);
            addFileUnderSpecificDir(parent, 3);
            parent = parent.getParent();
            System.out.println("b     " + parent);
        }

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(testPath,
            true);
        long count = 0;
        while (iterator.hasNext()) {
            count++;
            LocatedFileStatus status = iterator.next();
            System.out.println(status.toString());
            System.out.println("count: " + count);
        }
    }

    @Test
    // 路径或父目录及上级目录不存在，抛FileNotFoundException
    public void testListFilesAbnormal01() throws Exception {
        Path testFolder = getTestPath("test_dir");
        if (fs.exists(testFolder)) {
            fs.delete(testFolder, true);
        }

        boolean hasException = false;
        try {
            fs.listFiles(testFolder, false);
        } catch (FileNotFoundException e) {
            hasException = true;
        }
        assertTrue(hasException);
    }

    @Test
    // 路径的父目录及上级目录不是目录，抛AccessControlException
    public void testListFilesAbnormal02() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFolder = getTestPath("test_dir");
        FSDataOutputStream outputStream = fs.create(testFolder, false);
        outputStream.close();

        Path testFile = getTestPath("test_dir/test_file");

        boolean hasException = false;
        try {
            fs.listFiles(testFile, false);
        } catch (AccessControlException e) {
            hasException = true;
        }
        assertTrue(hasException);
    }

    private void addSubDirUnderSpecificDir(Path dir, int subDirNum)
        throws IOException {
        for (int i = 0; i < subDirNum; i++) {
            Path subDir = getTestPath(dir + "/" + SUB_DIR_PREFIX + i);
            fs.mkdirs(subDir);
        }
    }

    private void addFileUnderSpecificDir(Path dir, int fileNum)
        throws IOException {
        byte[] data = ContractTestUtils.dataset(8, 'a', 26);
        for (int i = 0; i < fileNum; i++) {
            Path file = getTestPath(dir + "/" + SUB_DIR_FILE_SURFIX + i);
            FSDataOutputStream outputStream = fs.create(file, false);
            outputStream.write(data);
            outputStream.close();
        }
    }
}

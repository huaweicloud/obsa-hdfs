package org.apache.hadoop.fs.obs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ITestOBSFSDataOutputStream {
    private OBSFileSystem fs;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private final int fileSize4 = 4 * 1024 * 1024;

    private final int fileSize5 = 5 * 1024 * 1024;

    private final int fileSize6 = 6 * 1024 * 1024;

    private final boolean caclMd5;

    private final String blockPolicy;

    @Parameterized.Parameters
    public static Collection digestPolicy() {
        return Arrays.asList(
            new PolicyParam(false, OBSConstants.FAST_UPLOAD_BUFFER_DISK),
            new PolicyParam(false, OBSConstants.FAST_UPLOAD_BUFFER_ARRAY),
            new PolicyParam(false, OBSConstants.FAST_UPLOAD_BYTEBUFFER),
            new PolicyParam(true, OBSConstants.FAST_UPLOAD_BUFFER_DISK),
            new PolicyParam(true, OBSConstants.FAST_UPLOAD_BUFFER_ARRAY),
            new PolicyParam(true, OBSConstants.FAST_UPLOAD_BYTEBUFFER)); // 是否开启md5校验
    }

    public ITestOBSFSDataOutputStream(PolicyParam param) {
        this.caclMd5 = param.calcMd5;
        this.blockPolicy = param.blockPolicy;
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
        conf.set(OBSConstants.MULTIPART_SIZE, String.valueOf(5 * 1024 * 1024));
        conf.setBoolean(OBSConstants.OUTPUT_STREAM_ATTACH_MD5, caclMd5);
        conf.set(OBSConstants.FAST_UPLOAD_BUFFER, blockPolicy);
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
    // 执行write操作写入5字节，调用close前，未将数据刷到服务端；调用close后，强制将客户端数据刷到服务端；
    public void testClose() throws Exception {
        if(!fs.isFsBucket()) {
            return;
        }

        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile, false);
        assertFalse(fs.exists(testFile));
        byte[] data = {0, 1, 2, 3, 4};
        outputStream.write(data);

        outputStream.hflush();
        assertEquals(5, fs.getFileStatus(testFile).getLen());

        outputStream.write(data);
        assertEquals(5, fs.getFileStatus(testFile).getLen());

        outputStream.close();
        assertEquals(10, fs.getFileStatus(testFile).getLen());
        fs.delete(testFile, false);
    }

    @Test
    // create file write 5M ,write4M,write6M hflush ,write5M  hsync,write4M
    // 共24M
    public void testCombination001() throws Exception {
        // if (fs.getMetricSwitch()) {
            if (!fs.isFsBucket()) {
                return;
            }
            Path testFile = getTestPath("test_file");
            if (fs.exists(testFile)) {
                fs.delete(testFile);
            }
            FSDataOutputStream outputStream = fs.create(testFile);
            byte[] data5 = ContractTestUtils.dataset(fileSize5, 'a',
                26);
            outputStream.write(data5);
            byte[] data4 = ContractTestUtils.dataset(fileSize4, 'a',
                26);
            outputStream.write(data4);
            byte[] data6 = ContractTestUtils.dataset(fileSize6, 'a',
                26);
            outputStream.write(data6);
            outputStream.hflush();
            outputStream.write(data5);
            assertEquals(fileSize5 * 4, fs.getFileStatus(testFile).getLen());
            outputStream.hsync();
            assertEquals(fileSize5 * 4, fs.getFileStatus(testFile).getLen());

            outputStream.write(data4);
            outputStream.hsync();
            System.out.println(fs.getFileStatus(testFile).getLen());
            if (outputStream != null) {
                outputStream.close();
            }
            fs.delete(testFile, false);
        // }
    }

    @Test
    // create file write 5M close  ,apend10M, write6M ,write5M,write4M close,
    // 共30M
    public void testCombination002() throws Exception {
        // if (fs.getMetricSwitch()) {
            if (!fs.isFsBucket()) {
                return;
            }
            Path testFile = getTestPath("test_file");
            if (fs.exists(testFile)) {
                fs.delete(testFile);
            }

            FSDataOutputStream outputStream = fs.create(testFile);
            byte[] data5 = ContractTestUtils.dataset(fileSize5, 'a',
                26);
            outputStream.write(data5);
            outputStream.close();

            outputStream = fs.append(testFile);
            byte[] data10 = ContractTestUtils.dataset(fileSize5 * 2, 'a',
                26);
            outputStream.write(data10);


            byte[] data6 = ContractTestUtils.dataset(fileSize6, 'a',
                26);
            outputStream.write(data6);

            outputStream.write(data5);


            byte[] data4 = ContractTestUtils.dataset(fileSize4, 'a',
                26);
            outputStream.write(data4);

            outputStream.close();

            assertEquals(fileSize5 * 6, fs.getFileStatus(testFile).getLen());

            fs.delete(testFile, false);
        // }
    }

    @Test
    // create file write 5M close,apend4M, write6M,hflush,write10M,hflush,close,
    // 共25M
    public void testCombination003() throws Exception {
        // if (fs.getMetricSwitch()) {
            if (!fs.isFsBucket()) {
                return;
            }
            Path testFile = getTestPath("test_file");
            if (fs.exists(testFile)) {
                fs.delete(testFile);
            }

            FSDataOutputStream outputStream = fs.create(testFile);
            byte[] data5 = ContractTestUtils.dataset(fileSize5, 'a',
                26);
            outputStream.write(data5);
            outputStream.close();

            outputStream = fs.append(testFile);
            byte[] data4 = ContractTestUtils.dataset(fileSize4, 'a',
                26);
            outputStream.write(data4);

            byte[] data6 = ContractTestUtils.dataset(fileSize6, 'a',
                26);
            outputStream.write(data6);

            outputStream.hflush();
            assertEquals(fileSize5 * 3, fs.getFileStatus(testFile).getLen());

            byte[] data10 = ContractTestUtils.dataset(fileSize5 * 2, 'a',
                26);
            outputStream.write(data10);
            outputStream.hflush();
            outputStream.close();

            assertEquals(fileSize5 * 5, fs.getFileStatus(testFile).getLen());

            fs.delete(testFile, false);
        // }
    }

    public static class PolicyParam {
        boolean calcMd5;
        String blockPolicy;

        public PolicyParam(boolean calcMd5, String blockPolicy) {
            this.calcMd5 = calcMd5;
            this.blockPolicy = blockPolicy;
        }
    }
}

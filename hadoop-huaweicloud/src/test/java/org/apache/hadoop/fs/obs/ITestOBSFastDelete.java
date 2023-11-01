package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;

@RunWith(MockitoJUnitRunner.class)
public class ITestOBSFastDelete {
    OBSFileSystem fs;

    OBSFileSystem mockFs;

    ObsClient obsClient;

    ObsClient mockObsClient;

    static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private static String trashPathStr = "trash";

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        conf.setBoolean(OBSConstants.FAST_DELETE_ENABLE, true);
        conf.set(OBSConstants.FAST_DELETE_DIR, trashPathStr);
        conf.setLong(OBSConstants.RETRY_SLEEP_BASETIME, 2);
        fs = OBSTestUtils.createTestFileSystem(conf);
        obsClient = fs.getObsClient();
        initMock();
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null && fs.isFsBucket()) {
            fs.delete(new Path(testRootPath), true);
            fs.delete(new Path(fs.getFastDeleteDir()), true);
        }
        Mockito.reset(mockFs, mockObsClient);
    }

    private void initMock() {
        mockFs = Mockito.spy(fs);
        mockObsClient = Mockito.spy(obsClient);
        Whitebox.setInternalState(mockFs, obsClient, mockObsClient);
    }

    private Path getTestPath(String relativePath) {
        return new Path(testRootPath + "/ITestOBSTrash/" + relativePath);
    }

    @Test
    // 删除目录，校验trash路径下包含被删除的目录
    public void delFolderToTrash() throws IOException {
        if (!fs.isFsBucket()) {
            return;
        }
        final byte[] data = ContractTestUtils.dataset(1024, 'a', 'z');
        List<Path> pathList = new ArrayList<Path>();
        for (int i = 0; i < 3; i++) {
            String objectName = "objectINfolder-" + i;
            Path objectPath = getTestPath(objectName);
            ContractTestUtils.createFile(fs, objectPath, false, data);
            pathList.add(objectPath);
        }
        fs.delete(new Path(testRootPath), true);

        Path trashPath =
            new Path(OBSCommonUtils.maybeAddBeginningSlash(trashPathStr));
        for (Path path : pathList) {
            assertTrue(fs.exists(new Path(trashPath,
                OBSCommonUtils.pathToKey(fs, path))));
        }
    }

    @Test
    // 删除文件，校验trash路径下包含被删除的目录
    public void delObjectToTrash() throws IOException {
        if (!fs.isFsBucket()) {
            return;
        }
        final byte[] data = ContractTestUtils.dataset(1024, 'a', 'z');
        Path objectPath = getTestPath("test_delete_object");
        ContractTestUtils.createFile(fs, objectPath, false, data);
        fs.delete(objectPath, true);

        Path trashPath =
            new Path(OBSCommonUtils.maybeAddBeginningSlash(trashPathStr));
        assertTrue(fs.exists(new Path(trashPath,
            OBSCommonUtils.pathToKey(fs, objectPath))));
    }

    @Test
    // 开启trash机制，先后删除两个同名文件，验证第二个文件trash后的文件名中不包含冒号
    public void testTrashSameNameObject01() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testFile = getTestPath("test_file");
        FSDataOutputStream outputStream = fs.create(testFile, false);
        outputStream.close();
        fs.delete(testFile, false);

        outputStream = fs.create(testFile, false);
        outputStream.close();
        fs.delete(testFile, false);

        Path trashPath =
            new Path(OBSCommonUtils.maybeAddBeginningSlash(trashPathStr));
        Path trashDir =
            new Path(trashPath, OBSCommonUtils.pathToKey(fs,
                testFile.getParent()));
        FileStatus[] files = fs.listStatus(trashDir);
        assertTrue(files.length >= 2);
        for (FileStatus file : files) {
            String path = file.getPath().toString();
            assertFalse(path.substring(path.indexOf("://") + 1).contains(":"));
        }

        fs.delete(trashDir, true);
    }

    @Test
    // 开启trash机制，先后删除两个同名非空目录，验证第二个目录trash后的目录名中不包含冒号
    public void testTrashSameNameObject02() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testDir = getTestPath("test_dir/");
        Path testFile = getTestPath("test_dir/test_file");
        fs.mkdirs(testDir);
        FSDataOutputStream outputStream = fs.create(testFile, false);
        outputStream.close();
        fs.delete(testDir, true);

        fs.mkdirs(testDir);
        outputStream = fs.create(testFile, false);
        outputStream.close();
        fs.delete(testDir, true);

        Path trashPath =
            new Path(OBSCommonUtils.maybeAddBeginningSlash(trashPathStr));
        Path trashDir =
            new Path(trashPath, OBSCommonUtils.pathToKey(fs,
                testDir.getParent()));
        FileStatus[] files = fs.listStatus(trashDir);
        assertTrue(files.length >= 2);
        for (FileStatus file : files) {
            String path = file.getPath().toString();
            assertFalse(path.substring(path.indexOf("://") + 1).contains(":"));
        }

        fs.delete(trashDir, true);
    }

    @Test
    // 开启trash机制，先后删除两个同名非空目录和文件，验证第二个文件trash后的文件名中不包含冒号
    public void testTrashSameNameObject03() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testDir = getTestPath("test_dir/");
        Path testFile = getTestPath("test_dir/test_file");
        fs.mkdirs(testDir);
        FSDataOutputStream outputStream = fs.create(testFile, false);
        outputStream.close();
        fs.delete(testDir, true);

        outputStream = fs.create(testDir, false);
        outputStream.close();
        fs.delete(testDir, true);

        Path trashPath =
            new Path(OBSCommonUtils.maybeAddBeginningSlash(trashPathStr));
        Path trashDir =
            new Path(trashPath, OBSCommonUtils.pathToKey(fs,
                testDir.getParent()));
        FileStatus[] files = fs.listStatus(trashDir);
        assertTrue(files.length >= 2);
        for (FileStatus file : files) {
            String path = file.getPath().toString();
            assertFalse(path.substring(path.indexOf("://") + 1).contains(":"));
        }

        fs.delete(trashDir, true);
    }

    @Test
    // 开启trash机制，先后删除两个同名非空目录和文件，验证第二个文件trash后的文件名中不包含冒号
    public void testTrashSameNameObject04() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        Path testDir = getTestPath("test_dir/");
        Path testFile = getTestPath("test_dir/test_file");
        fs.mkdirs(testDir);
        FSDataOutputStream outputStream = fs.create(testFile, false);
        outputStream.close();
        fs.delete(testDir, true);

        outputStream = fs.create(testDir, false);
        outputStream.close();
        fs.delete(testDir, true);

        Path trashPath =
            new Path(OBSCommonUtils.maybeAddBeginningSlash(trashPathStr));
        Path trashDir =
            new Path(trashPath, OBSCommonUtils.pathToKey(fs,
                testDir.getParent()));
        FileStatus[] files = fs.listStatus(trashDir);
        assertTrue(files.length >= 2);
        for (FileStatus file : files) {
            String path = file.getPath().toString();
            assertFalse(path.substring(path.indexOf("://") + 1).contains(":"));
        }

        fs.delete(trashDir, true);
    }

    @Test
    @Ignore
    // 测试多客户端执行trash到同一目录时，服务端返409，客户端添加时间戳重试
    public void testTrashWithConflict() throws Exception {

    }
}

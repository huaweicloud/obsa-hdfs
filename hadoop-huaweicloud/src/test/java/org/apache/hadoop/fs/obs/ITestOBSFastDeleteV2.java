package org.apache.hadoop.fs.obs;

import static org.apache.hadoop.fs.obs.OBSConstants.FAST_DELETE_VERSION_V2;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

@RunWith(MockitoJUnitRunner.class)
public class ITestOBSFastDeleteV2 extends ITestOBSFastDelete {

    private static String trashPathStr = "trashV2";

    @Before
    @Override
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        conf.setBoolean(OBSConstants.FAST_DELETE_ENABLE, true);
        conf.set(OBSConstants.FAST_DELETE_DIR, trashPathStr);
        conf.set(OBSConstants.FAST_DELETE_VERSION, FAST_DELETE_VERSION_V2);
        conf.setLong(OBSConstants.RETRY_SLEEP_BASETIME, 2);
        fs = OBSTestUtils.createTestFileSystem(conf);
        obsClient = fs.getObsClient();
        initMock();
    }

    private void initMock() {
        mockFs = Mockito.spy(fs);
        mockObsClient = Mockito.spy(obsClient);
        Whitebox.setInternalState(mockFs, obsClient, mockObsClient);
    }

    private Path getTestPath(String relativePath) {
        return new Path(testRootPath + "/ITestOBSTrash/" + relativePath);
    }

    private Path formatTrashPathV2(String trashPathStr) {
        SimpleDateFormat dateFmt = new SimpleDateFormat(OBSConstants.FAST_DELETE_VERSION_V2_CHECKPOINT_FORMAT);
        String checkpointStr = dateFmt.format(new Date());
        String checkpointDir = String.format(Locale.ROOT, "%s%s/",
            OBSCommonUtils.maybeAddTrailingSlash(trashPathStr), checkpointStr);
        return new Path(OBSCommonUtils.maybeAddBeginningSlash(checkpointDir));
    }

    @Test
    @Override
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

        Path trashPath = formatTrashPathV2(trashPathStr);

        for (Path path : pathList) {
            assertTrue(fs.exists(new Path(trashPath,
                OBSCommonUtils.pathToKey(fs, path))));
        }
    }

    @Test
    @Override
    // 删除文件，校验trash路径下包含被删除的目录
    public void delObjectToTrash() throws IOException {
        if (!fs.isFsBucket()) {
            return;
        }
        final byte[] data = ContractTestUtils.dataset(1024, 'a', 'z');
        Path objectPath = getTestPath("test_delete_object");
        ContractTestUtils.createFile(fs, objectPath, false, data);
        fs.delete(objectPath, true);

        Path trashPath = formatTrashPathV2(trashPathStr);

        assertTrue(fs.exists(new Path(trashPath,
            OBSCommonUtils.pathToKey(fs, objectPath))));
    }


    @Test
    @Override
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

        Path trashPath = formatTrashPathV2(trashPathStr);
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
    @Override
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

        Path trashPath = formatTrashPathV2(trashPathStr);
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
    @Override
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

        Path trashPath = formatTrashPathV2(trashPathStr);
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
    @Override
    // 重复用例，不需要跑
    public void testTrashSameNameObject04() throws Exception {
    }

}

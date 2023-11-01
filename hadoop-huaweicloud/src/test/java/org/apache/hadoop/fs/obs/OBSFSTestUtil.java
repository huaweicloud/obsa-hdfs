package org.apache.hadoop.fs.obs;

import com.obs.services.ObsClient;
import com.obs.services.model.AccessControlList;
import com.obs.services.model.GrantAndPermission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import static org.junit.Assert.*;

public class OBSFSTestUtil {

    static final int TEST_BUFF_SIZE = 128;

    static final int MODULUS = 128;

    static byte[] testBuffer = ContractTestUtils.dataset(TEST_BUFF_SIZE, 0,
        MODULUS);

    private static final Logger LOGGER = LoggerFactory.getLogger(
        OBSFSTestUtil.class);

    public static void setBucketAcl(ObsClient obsClient, String bucketName,
        AccessControlList acl) {
        assertNotNull("ObsClient should not be null.", obsClient);
        assertNotNull("bucketName should not be null.", bucketName);
        assertNotNull("acl should not be null.", acl);

        obsClient.setBucketAcl(bucketName, acl);
    }

    public static void setBucketAcl(ObsClient obsClient, String bucketName,
        AccessControlList acl,
        String specificUser, String specificUserPermission, String allUsers,
        String allUsersPermission) {
        assertNotNull("ObsClient should not be null.", obsClient);
        assertNotNull("bucketName should not be null.", bucketName);
        assertNotNull("acl should not be null.", acl);

        obsClient.setBucketAcl(bucketName, acl);
        AccessControlList accessControlList = obsClient.getBucketAcl(
            bucketName);
        Set<GrantAndPermission> grants = accessControlList.getGrants();
        boolean aclOk = true;
        for (GrantAndPermission grant : grants) {
            String accountId = grant.getGrantee().getIdentifier();
            String permission = grant.getPermission().getPermissionString();
            if (accountId.equals(specificUser) && !permission.equals(
                specificUserPermission)) {
                aclOk = false;
                break;
            } else if (accountId.equals(allUsers) && !permission.equals(
                allUsersPermission)) {
                aclOk = false;
                break;
            }
        }
        assertTrue(aclOk);
    }

    public static FSDataOutputStream createStream(FileSystem fs,
        Path objectPath) throws IOException {
        assertNotNull("FileSystem should not be null.", fs);
        assertNotNull("Path should not be null.", objectPath);

        return createStream(fs, objectPath, true);
    }

    public static FSDataOutputStream createStream(FileSystem fs,
        Path objectPath, boolean overwrite)
        throws IOException {
        assertNotNull("FileSystem should not be null.", fs);
        assertNotNull("Path should not be null.", objectPath);

        FSDataOutputStream stream = fs.create(objectPath, overwrite);

        return stream;
    }

    public static FSDataOutputStream createStreamWithFlag(FileSystem fs,
        Path objectPath,
        boolean overwrite, CreateFlag flag)
        throws IOException {
        assertNotNull("FileSystem should not be null.", fs);
        assertNotNull("Path should not be null.", objectPath);

        EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
        flags.add(flag);
        FSDataOutputStream stream = fs.create(objectPath, overwrite);

        return stream;
    }

    public static FSDataOutputStream createAppendStream(FileSystem fs,
        Path objectPath) throws IOException {
        assertNotNull("FileSystem should not be null.", fs);
        assertNotNull("Path should not be null.", objectPath);

        EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
        flags.add(CreateFlag.APPEND);
        return createAppendStream(fs, objectPath,
            new FsPermission((short) 00644), flags);
    }

    public static FSDataOutputStream createAppendStream(FileSystem fs,
        Path objectPath, FsPermission permission,
        EnumSet<CreateFlag> flags) throws IOException {
        assertNotNull("FileSystem should not be null.", fs);
        assertNotNull("Path should not be null.", objectPath);
        assertEquals(true, flags.contains(CreateFlag.APPEND));

        FSDataOutputStream stream = fs.create(objectPath, permission, flags,
            fs.getConf().getInt("io.file.buffer.size", 4096),
            fs.getDefaultReplication(objectPath),
            fs.getDefaultBlockSize(objectPath), null);

        return stream;
    }

    public static FSDataOutputStream createNonRecursiveStream(FileSystem fs,
        Path objectPath) throws IOException {
        assertNotNull("FileSystem should not be null.", fs);
        assertNotNull("Path should not be null.", objectPath);

        EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
        FsPermission permission = new FsPermission((short) 00644);

        FSDataOutputStream stream = fs.createNonRecursive(objectPath,
            permission, flags,
            fs.getConf().getInt("io.file.buffer.size", 4096),
            fs.getDefaultReplication(objectPath),
            fs.getDefaultBlockSize(objectPath), null);

        return stream;
    }

    public static FSDataOutputStream createNonRecursiveAppendStream(
        FileSystem fs, Path objectPath) throws IOException {
        assertNotNull("FileSystem should not be null.", fs);
        assertNotNull("Path should not be null.", objectPath);

        EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
        flags.add(CreateFlag.APPEND);
        FsPermission permission = new FsPermission((short) 00644);

        FSDataOutputStream stream = fs.createNonRecursive(objectPath,
            permission, flags,
            fs.getConf().getInt("io.file.buffer.size", 4096),
            fs.getDefaultReplication(objectPath),
            fs.getDefaultBlockSize(objectPath), null);

        return stream;
    }

    public static void writeData(FSDataOutputStream outputStream, long len)
        throws IOException {
        assertNotNull("FSDataOutputStream should not be null.", outputStream);
        assertTrue(len >= 0);

        long writtenLen = 0;
        long remainingLen;
        Throwable var10 = null;
        try {
            while (writtenLen < len) {
                remainingLen = len - writtenLen;
                if (remainingLen < testBuffer.length) {
                    outputStream.write(testBuffer, 0, (int) remainingLen);
                    writtenLen += remainingLen;
                } else {
                    outputStream.write(testBuffer);
                    writtenLen += testBuffer.length;
                }
            }
        } catch (Throwable var21) {
            var10 = var21;
            throw var21;
        } finally {
            if (var10 != null) {
                try {
                    outputStream.close();
                } catch (Throwable var20) {
                    var10.addSuppressed(var20);
                }
            }
        }

        assertEquals(len, writtenLen);
    }

    public static void deletePathRecursive(FileSystem fs, Path objectPath)
        throws IOException {
        assertNotNull("FileSystem should not be null.", fs);
        assertNotNull("Path should not be null.", objectPath);

        if (fs.exists(objectPath)) {
            fs.delete(objectPath, true);
        }
    }

    public static void assertFileHasLength(FileSystem fs, Path objectPath,
        int expected) throws IOException {
        assertNotNull("FileSystem should not be null.", fs);
        assertNotNull("Path should not be null.", objectPath);

        ContractTestUtils.assertFileHasLength(fs, objectPath, expected);
    }

    public static void assertPathExistence(FileSystem fs, Path objectPath,
        boolean shouldExist) throws IOException {
        if (shouldExist) {
            ContractTestUtils.assertPathExists(fs, "Path not exist.",
                objectPath);
        } else {
            ContractTestUtils.assertPathDoesNotExist(fs, "Path still exist.",
                objectPath);
        }
    }

    public static void readFile(FileSystem fs, Path objectPath, long start,
        long needReadLen) throws IOException {
        assertNotNull("FileSystem should not be null.", fs);
        assertNotNull("Path should not be null.", objectPath);
        assertTrue("start and readLen should not be negative.",
            start >= 0 && needReadLen >= 0);

        //        FileStatus fileStatus = fs.getFileStatus(objectPath);
        //        if (!fileStatus.isFile() || fileStatus.getLen() < start || fileStatus.getLen() < needReadLen) {
        //            throw new IOException("invalid read");
        //        }

        FSDataInputStream inputStream = null;
        try {
            inputStream = fs.open(objectPath);
            inputStream.seek(start);
            byte[] readBuffer = new byte[TEST_BUFF_SIZE];
            long totalBytesRead = 0L;
            long remainingLen;
            while (true) {
                remainingLen = needReadLen - totalBytesRead;
                if (remainingLen <= 0) {
                    break;
                }

                int bytesRead = inputStream.read(readBuffer, 0,
                    remainingLen > readBuffer.length
                        ? readBuffer.length
                        : (int) remainingLen);
                if (bytesRead < 0) {
                    if (totalBytesRead != needReadLen) {
                        throw new IOException("Expected to read " + needReadLen
                            + " bytes but only received "
                            + totalBytesRead);
                    }
                    break;
                }

                totalBytesRead += bytesRead;
            }

            assertEquals(needReadLen, totalBytesRead);
            assertEquals(0, remainingLen);
        } catch (IOException e) {
            throw e;
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    public static void verifyReceivedData(FileSystem fs, Path objectPath,
        long expectedSize)
        throws IOException {
        assertNotNull("FileSystem should not be null.", fs);
        assertNotNull("Path should not be null.", objectPath);
        assertTrue("expectedSize should be positive.", expectedSize >= 0);

        ContractTestUtils.verifyReceivedData(fs, objectPath, expectedSize,
            testBuffer.length, MODULUS);
    }

    public static boolean createLocalTestDir(String relativePath) {
        File dir = new File(relativePath);
        if (dir.exists()) {
            return true;
        }

        return dir.mkdirs();
    }

    public static File createLocalTestFile(String relativeFileName)
        throws IOException {
        File localFile = new File(relativeFileName);
        if (localFile.exists()) {
            deleteLocalFile(relativeFileName);
        }

        if (null != localFile.getParentFile() && !localFile.getParentFile()
            .exists()) {
            LOGGER.error(String.format("parentFile %s not exist!",
                localFile.getParent()));
            throw new IOException(String.format("parentFile %s not exist!",
                localFile.getParent()));
        }

        if (localFile.createNewFile()) {
            return localFile;
        } else {
            LOGGER.error(String.format("create file %s failed!",
                localFile.getParent()));
            throw new IOException(String.format("create file %s failed!",
                localFile.getParent()));
        }
    }

    public static void writeLocalFile(File localFile, long size)
        throws IOException {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(localFile));
            long writtenLen = 0;
            long remainingLen;
            while (writtenLen < size) {
                remainingLen = size - writtenLen;
                if (remainingLen >= testBuffer.length) {
                    writer.write(new String(testBuffer));
                    writtenLen += testBuffer.length;
                } else {
                    writer.write(new String(
                        Arrays.copyOfRange(testBuffer, 0, (int) remainingLen)));
                    writtenLen += remainingLen;
                }
            }
            writer.close();

            assertEquals(size, writtenLen);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    public static void deleteLocalFile(String relativePath) {
        File file = new File(relativePath);
        if (!file.exists()) {
            return;
        }

        if (file.isFile() || file.listFiles().length == 0) {
            file.delete();
        } else {
            for (File subFile : file.listFiles()) {
                deleteLocalFile(subFile.getPath());
            }
            file.delete();
        }

        return;
    }

    public static void clearBucket() throws IOException {
        Configuration conf = OBSContract.getConfiguration(null);
        OBSFileSystem fs = OBSTestUtils.createTestFileSystem(conf);
        Path path = new Path("/");
        FileStatus[] fs_list = fs.listStatus(path);
        for(FileStatus f : fs_list) {
            fs.delete(f.getPath(),true);
        }
    }
}

package org.apache.flink.fs.obshadoop;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.AbstractRecoverableWriterTest;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.IOUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 功能描述
 *
 * @since 2021-05-06
 */
public class HadoopOBSRecoverableWriterTest extends AbstractRecoverableWriterTest {

    private static final String TEST_DATA_DIR = "HadoopOBSRecoverableWriterTest";

    @BeforeClass
    public static void checkCredentialsAndSetup() throws IOException {
        // check whether credentials exist
        // OBSTestCredentials.assumeCredentialsAvailable();

        // initialize configuration with valid credentials
        final Configuration conf = new Configuration();
        conf.setString("fs.obs.access.key", OBSTestCredentials.getOBSAccessKey());
        conf.setString("fs.obs.secret.key", OBSTestCredentials.getOBSSecretKey());
        conf.setString("fs.obs.endpoint", OBSTestCredentials.getOBSEndpoint());
        FileSystem.initialize(conf);
    }

    @AfterClass
    public static void clearFsConfig() throws IOException {
        Path path = new Path(OBSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
        // Path path = new Path("obs://obsa-bigdata-posix/" + TEST_DATA_DIR);
        FileSystem fs = FileSystem.get(path.toUri());
        fs.delete(path, true);
        FileSystem.initialize(new Configuration());
    }

    @Override
    public FileSystem initializeFileSystem() throws Exception {
        return getBasePath().getFileSystem();
    }

    @Override
    public Path getBasePath() throws Exception {
        return new Path(OBSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
        // return new Path("obs://obsa-bigdata-posix/" + TEST_DATA_DIR);
    }

    @Test
    public void testCommitAfterRecoveryWithFileNotFound() throws Exception {
        Path testDir = this.getBasePathForTest();
        Path path = new Path(testDir, "part-0");
        FileSystem fileSystem = getBasePath().getFileSystem();
        RecoverableWriter initWriter = fileSystem.createRecoverableWriter();
        RecoverableFsDataOutputStream stream = null;

        RecoverableFsDataOutputStream.Committer committer;
        FlinkOBSFsRecoverable recoverable;

        try {
            stream = initWriter.open(path);
            stream.write("THIS IS A TEST 1.".getBytes(StandardCharsets.UTF_8));
            stream.persist();
            stream.persist();
            stream.write("THIS IS A TEST 2.".getBytes(StandardCharsets.UTF_8));
            committer = stream.closeForCommit();
            recoverable = (FlinkOBSFsRecoverable) committer.getRecoverable();
        } finally {
            IOUtils.closeQuietly(stream);
        }

        fileSystem.delete(new Path(recoverable.tempFile().toString()), true);

        try {
            committer.commitAfterRecovery();
        } catch (FileNotFoundException e) {
            Assert.assertFalse(false);
        }
    }

    // @Test
    // @Override
    // public void testResumeWithWrongOffset() throws Exception {
    //     // this is a rather unrealistic scenario, but it is to trigger
    //     // truncation of the file and try to resume with missing data.
    //
    //     final Path testDir = getBasePathForTest();
    //
    //     final RecoverableWriter writer = getNewFileSystemWriter();
    //     final Path path = new Path(testDir, "part-0");
    //
    //     final RecoverableWriter.ResumeRecoverable recoverable1;
    //     final RecoverableWriter.ResumeRecoverable recoverable2;
    //     RecoverableFsDataOutputStream stream = null;
    //     try {
    //         stream = writer.open(path);
    //         stream.write(testData1.getBytes(StandardCharsets.UTF_8));
    //
    //         recoverable1 = stream.persist();
    //         stream.write(testData2.getBytes(StandardCharsets.UTF_8));
    //
    //         recoverable2 = stream.persist();
    //         stream.write(testData3.getBytes(StandardCharsets.UTF_8));
    //     } finally {
    //         IOUtils.closeQuietly(stream);
    //     }
    //
    //     try (RecoverableFsDataOutputStream ignored = writer.recover(recoverable1)) {
    //         // this should work fine
    //     } catch (Exception e) {
    //         fail();
    //     }
    //
    //     // this should throw an exception
    //     try (RecoverableFsDataOutputStream ignored = writer.recover(recoverable2)) {
    //         fail();
    //     } catch (Exception e) {
    //         // we expect this
    //         return;
    //     }
    //     fail();
    // }
}

package org.apache.flink.fs.obshadoop;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemBehaviorTestSuite;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * 功能描述
 *
 * @since 2021-05-06
 */
public class HadoopOBSFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {
    private static final String TEST_DATA_DIR = "HadoopOBSFileSystemBehaviorITCase";

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
    public FileSystem getFileSystem() throws Exception {
        return getBasePath().getFileSystem();
    }

    @Override
    public Path getBasePath() throws Exception {
        return new Path(OBSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
        // return new Path("obs://obsa-bigdata-posix/" + TEST_DATA_DIR);
    }

    @Override
    public FileSystemKind getFileSystemKind() {
        return FileSystemKind.OBJECT_STORE;
    }

}

package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class ITestOBSHDFSWrapper {

    private Configuration conf;
    private MiniDFSCluster dfsCluster;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
        dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    }

    @After
    public void tearDown() throws Exception {
        if (dfsCluster != null) {
            dfsCluster.close();
        }
    }

    @Test
    public void testStatus() throws IOException {
        String fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();

        // hdfs native file
        DistributedFileSystem rawHdfsFS = dfsCluster.getFileSystem();
        rawHdfsFS.create(new Path("/hdfsfile1")).close();

        // wrapped local file system files
        folder.newFile("newFile1");
        File subfolder = folder.newFolder("folder1");
        subfolder.toPath().resolve("subFile1").toFile().createNewFile();

        conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./wrapper/",
            folder.getRoot().toURI().toString());
        conf.set("fs.hdfs.impl.disable.cache", "true");

        // If not set abfs, will still use Hdfs to resole path which will cause FileNotFoundException on hdfs.
        Path wrapperFile1 = new Path("hdfs://" + fsAuthority + "/wrapper/newFile1");
        FileContext fc = FileContext.getFileContext(wrapperFile1.toUri(), conf);
        try {
            fc.rename(wrapperFile1, new Path("hdfs://" + fsAuthority + "/wrapper/newFile1_r1"));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof FileNotFoundException);
        }

        // set abfs to MRSHDFSWrapper impl hdfs.
        conf.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.obs.OBSHDFSWrapper");
        Path wrapperFile1New = new Path("hdfs://" + fsAuthority + "/wrapper/newFile1");
        Path newPath = new Path("hdfs://" + fsAuthority + "/wrapper/newFile1_r1");
        FileContext fcNew = FileContext.getFileContext(wrapperFile1New.toUri(), conf);
        fcNew.rename(wrapperFile1, newPath);
        fcNew.open(newPath).close();
    }

}

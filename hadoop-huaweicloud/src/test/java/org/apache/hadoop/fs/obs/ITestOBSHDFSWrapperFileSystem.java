package org.apache.hadoop.fs.obs;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSMainOperationsBaseTest;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class ITestOBSHDFSWrapperFileSystem extends FSMainOperationsBaseTest {

    static Configuration conf = OBSContract.getConfiguration(null);
    static String fsAuthority;
    static MiniDFSCluster dfsCluster;

    @BeforeClass
    public static void setUpper() throws Exception {
        conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
        conf.setLong("dfs.namenode.fs-limits.min-block-size",1000);
        dfsCluster =
            new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
        fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();
    }

    @Override
    protected FileSystem createFileSystem() throws Exception {

        // conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
        // conf.setLong("dfs.namenode.fs-limits.min-block-size",1000);
        // dfsCluster =
        //     new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
        // fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();
        // conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./test/",
        //         folder.getRoot().toURI().toString());

        // // hdfs native file
        DistributedFileSystem fSys = dfsCluster.getFileSystem();
        String testRootDir = getTestRootPath(fSys).toUri().getPath();
        conf.set("fs.hdfs.mounttable." + fsAuthority + ".link."+testRootDir+
            "/test/", conf.get(OBSTestConstants.TEST_FS_OBS_NAME) + "/test/");
        return fSys;
    }


    @AfterClass
    public static void tearDowner() {
        if (dfsCluster != null) {
            dfsCluster.close();
        }
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @Test
    public void testTrash() throws IOException {
        // Configuration conf = new HdfsConfiguration();
        // conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
        // try (MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
        //     String fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();
        //
        //     // hdfs native file
        //     DistributedFileSystem fSys = fSys;
            fSys.create(new Path("/hdfsfile1")).close();
            fSys.mkdirs(new Path("/hdfsdir"), FsPermission.createImmutable((short) 777));
            fSys.create(new Path("/hdfsdir/subfile1")).close();

            // wrapped local file system files
            folder.newFile("newFile1");
            File subfolder = folder.newFolder("folder1");
            subfolder.toPath().resolve("subFile1").toFile().createNewFile();


            conf.set("fs.trash.interval", "30");
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./wrapper/",
                folder.getRoot().toURI().toString());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./abc/",
                folder.getRoot().toURI() + "/folder1");
            conf.set("fs.hdfs.impl.disable.cache", "true");
            //file:C:\Users\L30002~1\AppData\Local\Temp\junit1625000454693617450/folder1
            FileSystem fs = new Path("hdfs://" + fsAuthority + "/").getFileSystem(conf);
            TrashPolicy trashPolicy = TrashPolicy.getInstance(conf, fs);

            // trash wrapper file
            {
                Assert.assertTrue(trashPolicy.moveToTrash(new Path("hdfs://" + fsAuthority + "/wrapper/newFile1")));
                Assert.assertTrue(trashPolicy.moveToTrash(new Path("hdfs://" + fsAuthority + "/abc/subFile1")));
                Assert.assertTrue(trashPolicy.moveToTrash(new Path("hdfs://" + fsAuthority + "/hdfsfile1")));
                Assert.assertTrue(trashPolicy.moveToTrash(new Path("hdfs://" + fsAuthority + "/hdfsdir/subfile1")));
            }

    }

    // trash target folder is wrapped to another filesystem
    @Test
    public void testTrashUser() throws IOException {
        // Configuration conf = new HdfsConfiguration();
        // try (MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
        //     String fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();
        //
        //     // hdfs native file
        //     DistributedFileSystem fSys = fSys;
            fSys.create(new Path("/hdfsfile1")).close();
            fSys.mkdirs(new Path("/hdfsdir"),
                FsPermission.createImmutable((short) 777));
            fSys.create(new Path("/hdfsdir/subfile1")).close();

            // wrapped local file system files
            folder.newFile("newFile1");
            File subfolder = folder.newFolder("folder1");
            subfolder.toPath().resolve("subFile1").toFile().createNewFile();

            conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
            conf.set("fs.trash.interval", "30");
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./wrapper/",
                folder.getRoot().toURI().toString());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./user/",
                folder.getRoot().toURI().toString() + "/folder1");
            conf.set("fs.hdfs.impl.disable.cache", "true");

            FileSystem fs = new Path("hdfs://" + fsAuthority + "/").getFileSystem(conf);
            TrashPolicy trashPolicy = TrashPolicy.getInstance(conf, fs);
            Path hdfsTrashRoot = fs.getTrashRoot(new Path("hdfs://" + fsAuthority + "/hdfsfile1"));
            Path localFSTrashRoot = fs.getTrashRoot(new Path("hdfs://" + fsAuthority + "/wrapper/newFile1"));
            fs.delete(localFSTrashRoot, true);
            fs.delete(hdfsTrashRoot, true);

            // trash wrapper file
            {
                Assert.assertTrue(trashPolicy.moveToTrash(new Path("hdfs://" + fsAuthority + "/wrapper/newFile1")));
                Assert.assertTrue(trashPolicy.moveToTrash(new Path("hdfs://" + fsAuthority + "/user/subFile1")));
                Assert.assertTrue(trashPolicy.moveToTrash(new Path("hdfs://" + fsAuthority + "/hdfsfile1")));
                Assert.assertTrue(trashPolicy.moveToTrash(new Path("hdfs://" + fsAuthority + "/hdfsdir/subfile1")));
            }
            // list trash root
            {
                RemoteIterator<LocatedFileStatus> hdfsTrashItr = fs.listFiles(hdfsTrashRoot, true);
                int countHdfs = 0;
                while (hdfsTrashItr.hasNext()) {
                    System.out.println(hdfsTrashItr.next());
                    countHdfs++;
                }
                Assert.assertEquals(2, countHdfs);

                RemoteIterator<LocatedFileStatus> localTrashItr = fs.listFiles(localFSTrashRoot, true);
                int countLocal = 0;
                while (localTrashItr.hasNext()) {
                    System.out.println(localTrashItr.next());
                    countLocal++;
                }
                Assert.assertEquals(2, countLocal);
            }
        // }
    }

    @Test
    public void testRename() throws IOException {
        // Configuration conf = new HdfsConfiguration();
        // try (MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
        //     String fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();
        //
        //     // hdfs native file
        //     DistributedFileSystem fSys = fSys;
            fSys.create(new Path("/hdfsfile1")).close();
            fSys.create(new Path("/hdfsfile2")).close();
            fSys.create(new Path("/hdfsfile3")).close();
            fSys.mkdirs(new Path("/hdfsdir"),
                FsPermission.createImmutable((short) 777));
            fSys.create(new Path("/hdfsdir/subfile1")).close();
            fSys.create(new Path("/hdfsdir/subfile2")).close();

            // wrapped local file system files
            folder.newFile("newFile1");
            folder.newFile("newFile2");
            folder.newFile("newFile3");
            File subfolder = folder.newFolder("folder1");
            System.out.println("---"+subfolder.toPath());
            subfolder.toPath().resolve("subFile1").toFile().createNewFile();
            subfolder.toPath().resolve("subFile2").toFile().createNewFile();

            conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./wrapper/",
                folder.getRoot().toURI().toString());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./abc/",
                folder.getRoot().toURI().toString() + "/folder1");
            conf.set("fs.hdfs.impl.disable.cache", "true");

            FileSystem fs = new Path("hdfs://" + fsAuthority + "/").getFileSystem(conf);

            // cross fs rename
            {
                Path from = new Path("hdfs://" + fsAuthority + "/hdfsfile1");
                Path to = new Path("hdfs://" + fsAuthority + "/wrapper/hdfsfile1");
                try {
                    fs.rename(from, to);
                    fail("cross fs rename should fail.");
                } catch (IOException e) {
                    // 20210729: [BUG2021070600630] change UnsupportedOperationException to IOException
                    if (!(e.getCause() instanceof UnsupportedOperationException)) {
                        throw e;
                    }
                    // ignore UnsupportedOperationException cause
                }
            }

            // same fs rename in hdfs
            {
                Path from = new Path("hdfs://" + fsAuthority + "/hdfsfile1");
                Path to = new Path("hdfs://" + fsAuthority + "/hdfsfile1");
                Assert.assertTrue(fs.rename(from, to));
                Assert.assertTrue(fSys.exists(to));
            }

            // same fs rename in wrapper fs
            {
                Path from = new Path("hdfs://" + fsAuthority + "/wrapper/newFile1");
                Path to = new Path("hdfs://" + fsAuthority + "/wrapper/newfile11");
                Assert.assertTrue(fs.rename(from, to));
                Assert.assertTrue(folder.getRoot().toPath().resolve("newfile11").toFile().exists());
            }

            // same fs rename in wrapper fs to different mount point
            {
                Path from = new Path("hdfs://" + fsAuthority + "/wrapper/newFile2");
                Path to = new Path("hdfs://" + fsAuthority + "/abc/newFile2");
                Assert.assertTrue(fs.rename(from, to));
                Assert.assertTrue(folder.getRoot().toPath().resolve("folder1/newFile2").toFile().exists());
            }
        // }
    }

    @Test
    public void testRename1() throws IOException {
        // Configuration conf = new HdfsConfiguration();
        // try (MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
        //     String fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();
        //
        //     // hdfs native file
        //     DistributedFileSystem fSys = fSys;
            fSys.create(new Path("/hdfsfile1")).close();
            fSys.create(new Path("/hdfsfile2")).close();
            fSys.create(new Path("/hdfsfile3")).close();
            fSys.mkdirs(new Path("/hdfsdir"),
                FsPermission.createImmutable((short) 777));
            fSys.create(new Path("/hdfsdir/subfile1")).close();
            fSys.create(new Path("/hdfsdir/subfile2")).close();

            // wrapped local file system files
            folder.newFile("newFile1");
            folder.newFile("newFile2");
            folder.newFile("newFile3");
            folder.newFile("newFile4");
            folder.newFile("newFile5");
            File subfolder = folder.newFolder("folder1");
            subfolder.toPath().resolve("subFile1").toFile().createNewFile();
            subfolder.toPath().resolve("subFile2").toFile().createNewFile();
            subfolder.toPath().resolve("newFile5").toFile().createNewFile();

            conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./wrapper/",
                folder.getRoot().toURI().toString());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./abc/",
                folder.getRoot().toURI().toString() + "/folder1");
            conf.set("fs.hdfs.impl.disable.cache", "true");

            FileSystem fs = new Path("hdfs://" + fsAuthority + "/").getFileSystem(conf);

            // cross fs rename
            {
                Path from = new Path("hdfs://" + fsAuthority + "/hdfsfile1");
                Path to = new Path("hdfs://" + fsAuthority + "/wrapper/hdfsfile1");
                try {
                    if (fs instanceof OBSHDFSFileSystem) {
                        ((OBSHDFSFileSystem) fs).rename(from, to, Options.Rename.OVERWRITE);
                    } else {
                        fs.delete(to, false);
                        fs.rename(from, to);
                    }
                    fail("cross fs rename should fail.");
                } catch (IOException e) {
                    // 20210729: [BUG2021070600630] change UnsupportedOperationException to IOException
                    if (!(e.getCause() instanceof UnsupportedOperationException)) {
                        throw e;
                    }
                    // ignore UnsupportedOperationException cause
                }
            }

            // same fs rename in hdfs
            {
                Path from = new Path("hdfs://" + fsAuthority + "/hdfsfile1");
                Path to = new Path("hdfs://" + fsAuthority + "/hdfsfile11");
                boolean success = false;
                if (fs instanceof OBSHDFSFileSystem) {
                    ((OBSHDFSFileSystem) fs).rename(from, to, Options.Rename.OVERWRITE);
                    success = true;
                } else {
                    fs.delete(to, false);
                    success = fs.rename(from, to);
                }
                Assert.assertTrue(success);
                Assert.assertTrue(fSys.exists(to));
            }

            // same fs rename in wrapper fs
            {
                Path from = new Path("hdfs://" + fsAuthority + "/wrapper/newFile1");
                Path to = new Path("hdfs://" + fsAuthority + "/wrapper/newfile11");
                boolean success = false;
                if (fs instanceof OBSHDFSFileSystem) {
                    ((OBSHDFSFileSystem) fs).rename(from, to, Options.Rename.OVERWRITE);
                    success = true;
                } else {
                    fs.delete(to, false);
                    success = fs.rename(from, to);
                }
                Assert.assertTrue(success);
                Assert.assertTrue(folder.getRoot().toPath().resolve("newfile11").toFile().exists());
            }

            // same fs rename in wrapper fs, to path is exist
            {
                Path from = new Path("hdfs://" + fsAuthority + "/wrapper/newFile4");
                Path to = new Path("hdfs://" + fsAuthority + "/wrapper/newFile4");
                boolean success = false;
                if (fs instanceof OBSHDFSFileSystem) {
                    ((OBSHDFSFileSystem) fs).rename(from, to, Options.Rename.OVERWRITE);
                    success = true;
                } else {
                    fs.delete(to, false);
                    success = fs.rename(from, to);
                }
                Assert.assertTrue(success);
                Assert.assertTrue(folder.getRoot().toPath().resolve("newFile4").toFile().exists());
            }

            // same fs rename in wrapper fs to different mount point
            {
                Path from = new Path("hdfs://" + fsAuthority + "/wrapper/newFile2");
                Path to = new Path("hdfs://" + fsAuthority + "/abc/newFile2");
                boolean success = false;
                if (fs instanceof OBSHDFSFileSystem) {
                    ((OBSHDFSFileSystem) fs).rename(from, to, Options.Rename.OVERWRITE);
                    success = true;
                } else {
                    fs.delete(to, false);
                    success = fs.rename(from, to);
                }
                Assert.assertTrue(success);
                Assert.assertTrue(folder.getRoot().toPath().resolve("folder1/newFile2").toFile().exists());
            }

            // same fs rename in wrapper fs to different mount point, to path is exist
            {
                Path from = new Path("hdfs://" + fsAuthority + "/wrapper/newFile5");
                Path to = new Path("hdfs://" + fsAuthority + "/abc/newFile5");
                boolean success = false;
                if (fs instanceof OBSHDFSFileSystem) {
                    ((OBSHDFSFileSystem) fs).rename(from, to, Options.Rename.OVERWRITE);
                    success = true;
                } else {
                    fs.delete(to, false);
                    success = fs.rename(from, to);
                }
                Assert.assertTrue(success);
                Assert.assertTrue(folder.getRoot().toPath().resolve("folder1/newFile5").toFile().exists());
            }


        // }
    }

    @Test
    public void testDelete() throws IOException {
        // Configuration conf = new HdfsConfiguration();
        // try (MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
        //     String fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();

            conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./wrapper/",
                folder.getRoot().toURI().toString());
            conf.set("fs.hdfs.impl.disable.cache", "true");

            FileSystem fs = new Path("hdfs://" + fsAuthority + "/").getFileSystem(conf);

            File dir = folder.newFolder("toBeDelete");
            dir.toPath().resolve("subFile1").toFile().createNewFile();
            dir.toPath().resolve("subFile2").toFile().createNewFile();

            Path deletePath = new Path("hdfs://" + fsAuthority + "/wrapper/toBeDelete");
            Assert.assertTrue(fs.delete(deletePath, true));

            try {
                fs.listStatus(deletePath);
                fail("the path " + deletePath + " should not exists");
            } catch (FileNotFoundException e) {
                // success
            }

            FileStatus[] status = fs.listStatus(deletePath.getParent());
            Assert.assertEquals(0, status.length);
        // }
    }

    @Test
    public void testReadWriteFiles() throws IOException {
        // Configuration conf = new HdfsConfiguration();
        // try (MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
        //     String fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();
        
            // conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./wrapper/",
                folder.getRoot().toURI().toString());
            // conf.set("fs.hdfs.impl.disable.cache", "true");

            FileSystem fs = new Path("hdfs://" + fsAuthority + "/").getFileSystem(conf);

            Path fileInWrapper = new Path("/wrapper/file1");
            Path fileInHDFS = new Path("/hdfsfile1");

            byte[] bytes = generateByteArry(4096);

            try (FSDataOutputStream out = fs.create(fileInWrapper)) {
                out.write(bytes);
            } catch (IOException e) {
                fail("write file failed");
            }

            try (FSDataOutputStream out = fs.create(fileInHDFS)) {
                out.write(bytes);
            } catch (IOException e) {
                fail("write file failed");
            }

            byte[] resultWrapper = new byte[4096];
            try (FSDataInputStream in = fs.open(fileInHDFS)) {
                in.read(resultWrapper);
            }

            byte[] resultHdfs = new byte[4096];
            try (FSDataInputStream in = fs.open(fileInHDFS)) {
                in.read(resultHdfs);
            }

            Assert.assertArrayEquals(bytes, resultHdfs);
            Assert.assertArrayEquals(bytes, resultWrapper);
        // }
    }



    @Test
    public void testListStatus() throws IOException {
        // Configuration conf = new HdfsConfiguration();
        // try (MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
        //     String fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();

            conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./wrapper/",
                folder.getRoot().toURI().toString());
            conf.set("fs.hdfs.impl.disable.cache", "true");

            // hdfs native file
            fSys.create(new Path("/hdfsfile1")).close();
            fSys.create(new Path("/hdfsfile2")).close();
            fSys.create(new Path("/hdfsfile3")).close();
            fSys.mkdirs(new Path("/hdfsdir"),
                FsPermission.createImmutable((short) 777));
            fSys.create(new Path("/hdfsdir/subfile1")).close();
            fSys.create(new Path("/hdfsdir/subfile2")).close();

            // wrapped local file system files
            folder.newFile("newFile1");
            folder.newFile("newFile2");
            folder.newFile("newFile3");
            File subfolder = folder.newFolder("folder1");
            subfolder.toPath().resolve("subFile1").toFile().createNewFile();
            subfolder.toPath().resolve("subFile2").toFile().createNewFile();

            FileSystem fs = new Path("hdfs://" + fsAuthority + "/").getFileSystem(conf);
            Path listPath = new Path("hdfs://" + fsAuthority + "/wrapper");
            Path listPathWithTailSlash = new Path("hdfs://" + fsAuthority + "/wrapper/");
            FileStatus[] files = fs.listStatus(listPath);
            Set<String> filesSet = Arrays.stream(files).map(FileStatus::getPath).map(Path::toString).collect(Collectors.toSet());
            Assert.assertEquals(4, files.length);
            Assert.assertTrue(filesSet.contains(new Path(listPath, "newFile1").toString()));
            Assert.assertTrue(filesSet.contains(new Path(listPath, "newFile2").toString()));
            Assert.assertTrue(filesSet.contains(new Path(listPath, "newFile3").toString()));
            Assert.assertTrue(filesSet.contains(new Path(listPath, "folder1").toString()));

            FileStatus[] files2 = fs.listStatus(listPathWithTailSlash);
            Set<String> filesSet2 = Arrays.stream(files2).map(FileStatus::getPath).map(Path::toString).collect(Collectors.toSet());
            Assert.assertEquals(4, files2.length);
            Assert.assertTrue(filesSet2.contains(new Path(listPath, "newFile1").toString()));
            Assert.assertTrue(filesSet2.contains(new Path(listPath, "newFile2").toString()));
            Assert.assertTrue(filesSet2.contains(new Path(listPath, "newFile3").toString()));
            Assert.assertTrue(filesSet2.contains(new Path(listPath, "folder1").toString()));


            RemoteIterator<LocatedFileStatus> recursivelistIrt = fs.listFiles(listPath, true);
            Set<String> recursivelistPaths = new HashSet<>();
            while (recursivelistIrt.hasNext()) {
                recursivelistPaths.add(recursivelistIrt.next().getPath().toString());
            }

            Assert.assertTrue(recursivelistPaths.contains(new Path(listPath, "newFile1").toString()));
            Assert.assertTrue(recursivelistPaths.contains(new Path(listPath, "newFile2").toString()));
            Assert.assertTrue(recursivelistPaths.contains(new Path(listPath, "newFile3").toString()));
            Assert.assertTrue(recursivelistPaths.contains(new Path(listPath, "folder1/subFile1").toString()));
            Assert.assertTrue(recursivelistPaths.contains(new Path(listPath, "folder1/subFile2").toString()));

            RemoteIterator<LocatedFileStatus> nonRecursivelistIrt = fs.listFiles(listPath, false);
            Set<String> nonRecursivelistPaths = new HashSet<>();
            while (nonRecursivelistIrt.hasNext()) {
                nonRecursivelistPaths.add(nonRecursivelistIrt.next().getPath().toString());
            }

            Assert.assertTrue(nonRecursivelistPaths.contains(new Path(listPath, "newFile1").toString()));
            Assert.assertTrue(nonRecursivelistPaths.contains(new Path(listPath, "newFile2").toString()));
            Assert.assertTrue(nonRecursivelistPaths.contains(new Path(listPath, "newFile3").toString()));

            Path listHDFSPath = new Path("hdfs://" + fsAuthority + "/");
            FileStatus[] hdfsFiles = fs.listStatus(listHDFSPath);
            Set<String> filesSetHdfs = Arrays.stream(hdfsFiles).map(FileStatus::getPath).map(Path::toString).collect(Collectors.toSet());
            Assert.assertEquals(5, filesSetHdfs.size());
            Assert.assertTrue(filesSetHdfs.contains(new Path(listHDFSPath, "hdfsfile1").toString()));
            Assert.assertTrue(filesSetHdfs.contains(new Path(listHDFSPath, "hdfsfile2").toString()));
            Assert.assertTrue(filesSetHdfs.contains(new Path(listHDFSPath, "hdfsfile3").toString()));
            Assert.assertTrue(filesSetHdfs.contains(new Path(listHDFSPath, "hdfsdir").toString()));

            RemoteIterator<LocatedFileStatus> recursivelistHDFSIrt = fs.listFiles(listHDFSPath, true);
            Set<String> recursivelistHDFSPaths = new HashSet<>();
            while (recursivelistHDFSIrt.hasNext()) {
                recursivelistHDFSPaths.add(recursivelistHDFSIrt.next().getPath().toString());
            }
            Assert.assertEquals(7, recursivelistHDFSPaths.size());
            Assert.assertTrue(recursivelistHDFSPaths.contains(new Path(listHDFSPath, "hdfsfile1").toString()));
            Assert.assertTrue(recursivelistHDFSPaths.contains(new Path(listHDFSPath, "hdfsfile2").toString()));
            Assert.assertTrue(recursivelistHDFSPaths.contains(new Path(listHDFSPath, "hdfsfile3").toString()  ));
            Assert.assertTrue(recursivelistHDFSPaths.contains(new Path(listHDFSPath, "hdfsdir/subfile1").toString()));
            Assert.assertTrue(recursivelistHDFSPaths.contains(new Path(listHDFSPath, "hdfsdir/subfile2").toString()));
            fSys.deleteOnExit(new Path("/wrapper"));
        // }
    }

    @Test
    public void testListStatus1() throws IOException {
        // Configuration conf = new HdfsConfiguration();
        // try (MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
        //     String fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();

            conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link"
                + "./wrapper1234/", folder.getRoot().toURI().toString() + "/test1234");
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./wrapper",
                folder.getRoot().toURI().toString() + "/test1234");
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./wrapper"
                + "/1234/", folder.getRoot().toURI().toString() + "/test12345");
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./yy/",
                folder.getRoot().toURI().toString() + "/folder1");
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./wrapper/",
                folder.getRoot().toURI().toString());
            conf.set("fs.hdfs.impl.disable.cache", "true");

            // hdfs native file
            fSys.create(new Path("/hdfsfile1")).close();
            fSys.create(new Path("/hdfsfile2")).close();
            fSys.create(new Path("/hdfsfile3")).close();
            fSys.mkdirs(new Path("/hdfsdir"),
                FsPermission.createImmutable((short) 777));
            fSys.create(new Path("/hdfsdir/subfile1")).close();
            fSys.create(new Path("/hdfsdir/subfile2")).close();

            // wrapped local file system files
            folder.newFile("newFile1");
            folder.newFile("newFile2");
            folder.newFile("newFile3");
            File subfolder = folder.newFolder("folder1");
            subfolder.toPath().resolve("subFile1").toFile().createNewFile();
            subfolder.toPath().resolve("subFile2").toFile().createNewFile();

            FileSystem fs = new Path("hdfs://" + fsAuthority + "/").getFileSystem(conf);
            Path listPath = new Path("hdfs://" + fsAuthority + "/yy");

            FileStatus[] status = fs.listStatus(listPath);
            Assert.assertEquals(2, status.length);
            fSys.deleteOnExit(new Path("/wrapper"));
        // }
    }

    @Test
    public void testListLocatedStatus() throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        // Configuration conf = new HdfsConfiguration();
        // try (MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
        //     String fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();

            conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./wrapper/",
                folder.getRoot().toURI().toString());
            conf.set("fs.hdfs.impl.disable.cache", "true");

            // hdfs native file
            fSys.create(new Path("/hdfsfile1")).close();
            fSys.create(new Path("/hdfsfile2")).close();
            fSys.create(new Path("/hdfsfile3")).close();
            fSys.mkdirs(new Path("/hdfsdir"),
                FsPermission.createImmutable((short) 777));
            fSys.create(new Path("/hdfsdir/subfile1")).close();
            fSys.create(new Path("/hdfsdir/subfile2")).close();

            // wrapped local file system files
            folder.newFile("newFile1");
            folder.newFile("newFile2");
            folder.newFile("newFile3");
            File subfolder = folder.newFolder("folder1");
            subfolder.toPath().resolve("subFile1").toFile().createNewFile();
            subfolder.toPath().resolve("subFile2").toFile().createNewFile();

            FileSystem fs = new Path("hdfs://" + fsAuthority + "/").getFileSystem(conf);
            Path listPath = new Path("hdfs://" + fsAuthority + "/wrapper");
            Path listPathWithTailSlash = new Path("hdfs://" + fsAuthority + "/wrapper/");
            RemoteIterator<LocatedFileStatus> files = fs.listLocatedStatus(listPath);
            List<LocatedFileStatus> locatedStatusList = new ArrayList<>();
            while (files.hasNext()) {
                locatedStatusList.add(files.next());
            }

            Set<String> filesSet = locatedStatusList.stream().map(FileStatus::getPath).map(Path::toString).collect(Collectors.toSet());
            Assert.assertEquals(4, locatedStatusList.size());
            Assert.assertTrue(filesSet.contains(new Path(listPath, "newFile1").toString()));
            Assert.assertTrue(filesSet.contains(new Path(listPath, "newFile2").toString()));
            Assert.assertTrue(filesSet.contains(new Path(listPath, "newFile3").toString()));
            Assert.assertTrue(filesSet.contains(new Path(listPath, "folder1").toString()));

            RemoteIterator<LocatedFileStatus> files2 = fs.listLocatedStatus(listPathWithTailSlash);
            List<LocatedFileStatus> locatedStatusList2 = new ArrayList<>();
            while (files2.hasNext()) {
                locatedStatusList2.add(files2.next());
            }
            Set<String> filesSet2 = locatedStatusList2.stream().map(FileStatus::getPath).map(Path::toString).collect(Collectors.toSet());
            Assert.assertEquals(4, locatedStatusList2.size());
            Assert.assertTrue(filesSet2.contains(new Path(listPath, "newFile1").toString()));
            Assert.assertTrue(filesSet2.contains(new Path(listPath, "newFile2").toString()));
            Assert.assertTrue(filesSet2.contains(new Path(listPath, "newFile3").toString()));
            Assert.assertTrue(filesSet2.contains(new Path(listPath, "folder1").toString()));
        fSys.deleteOnExit(new Path("/wrapper"));
        // }
    }

    @Test
    public void testListStatus2() throws IOException {
        // Configuration conf = new HdfsConfiguration();
        // try (MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
        //     String fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();

            conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./ ",
                folder.getRoot().toURI().toString());
            conf.set("fs.hdfs.impl.disable.cache", "true");

            // hdfs native file
            fSys.create(new Path("/hdfsfile1")).close();
            fSys.create(new Path("/hdfsfile2")).close();
            fSys.create(new Path("/hdfsfile3")).close();
            fSys.mkdirs(new Path("/hdfsdir"),
                FsPermission.createImmutable((short) 777));
            fSys.create(new Path("/hdfsdir/subfile1")).close();
            fSys.create(new Path("/hdfsdir/subfile2")).close();

            // wrapped local file system files
            folder.newFile("newFile1");
            folder.newFile("newFile2");
            folder.newFile("newFile3");
            File subfolder = folder.newFolder("folder1");
            subfolder.toPath().resolve("subFile1").toFile().createNewFile();
            subfolder.toPath().resolve("subFile2").toFile().createNewFile();

            FileSystem fs = new Path("hdfs://" + fsAuthority + "/").getFileSystem(conf);
            Path listPath = new Path("hdfs://" + fsAuthority + "/");
            Path listPathWithTailSlash = new Path("hdfs://" + fsAuthority + "/");
            FileStatus[] files = fs.listStatus(listPath);
            Set<String> filesSet = Arrays.stream(files).map(FileStatus::getPath).map(Path::toString).collect(Collectors.toSet());
            Assert.assertEquals(4, files.length);
            Assert.assertTrue(filesSet.contains(new Path(listPath, "newFile1").toString()));
            Assert.assertTrue(filesSet.contains(new Path(listPath, "newFile2").toString()));
            Assert.assertTrue(filesSet.contains(new Path(listPath, "newFile3").toString()));
            Assert.assertTrue(filesSet.contains(new Path(listPath, "folder1").toString()));

            FileStatus[] files2 = fs.listStatus(listPathWithTailSlash);
            Set<String> filesSet2 = Arrays.stream(files2).map(FileStatus::getPath).map(Path::toString).collect(Collectors.toSet());
            Assert.assertEquals(4, files2.length);
            Assert.assertTrue(filesSet2.contains(new Path(listPath, "newFile1").toString()));
            Assert.assertTrue(filesSet2.contains(new Path(listPath, "newFile2").toString()));
            Assert.assertTrue(filesSet2.contains(new Path(listPath, "newFile3").toString()));
            Assert.assertTrue(filesSet2.contains(new Path(listPath, "folder1").toString()));


            RemoteIterator<LocatedFileStatus> recursivelistIrt = fs.listFiles(listPath, true);
            Set<String> recursivelistPaths = new HashSet<>();
            while (recursivelistIrt.hasNext()) {
                recursivelistPaths.add(recursivelistIrt.next().getPath().toString());
            }

            Assert.assertTrue(recursivelistPaths.contains(new Path(listPath, "newFile1").toString()));
            Assert.assertTrue(recursivelistPaths.contains(new Path(listPath, "newFile2").toString()));
            Assert.assertTrue(recursivelistPaths.contains(new Path(listPath, "newFile3").toString()));
            Assert.assertTrue(recursivelistPaths.contains(new Path(listPath, "folder1/subFile1").toString()));
            Assert.assertTrue(recursivelistPaths.contains(new Path(listPath, "folder1/subFile2").toString()));

            RemoteIterator<LocatedFileStatus> nonRecursivelistIrt = fs.listFiles(listPath, false);
            Set<String> nonRecursivelistPaths = new HashSet<>();
            while (nonRecursivelistIrt.hasNext()) {
                nonRecursivelistPaths.add(nonRecursivelistIrt.next().getPath().toString());
            }

            Assert.assertTrue(nonRecursivelistPaths.contains(new Path(listPath, "newFile1").toString()));
            Assert.assertTrue(nonRecursivelistPaths.contains(new Path(listPath, "newFile2").toString()));
            Assert.assertTrue(nonRecursivelistPaths.contains(new Path(listPath, "newFile3").toString()));

            Path listHDFSPath = new Path("hdfs://" + fsAuthority + "/");
            FileStatus[] hdfsFiles = fs.listStatus(listHDFSPath);
            Set<String> filesSetHdfs = Arrays.stream(hdfsFiles).map(FileStatus::getPath).map(Path::toString).collect(Collectors.toSet());
            Assert.assertEquals(4, filesSetHdfs.size());
            Assert.assertTrue(filesSetHdfs.contains(new Path(listHDFSPath, "folder1").toString()));
            Assert.assertTrue(filesSetHdfs.contains(new Path(listHDFSPath, "newFile3").toString()));
            Assert.assertTrue(filesSetHdfs.contains(new Path(listHDFSPath, "newFile2").toString()));
            Assert.assertTrue(filesSetHdfs.contains(new Path(listHDFSPath, "newFile1").toString()));

            RemoteIterator<LocatedFileStatus> recursivelistHDFSIrt = fs.listFiles(listHDFSPath, true);
            Set<String> recursivelistHDFSPaths = new HashSet<>();
            while (recursivelistHDFSIrt.hasNext()) {
                recursivelistHDFSPaths.add(recursivelistHDFSIrt.next().getPath().toString());
            }
            Assert.assertEquals(5, recursivelistHDFSPaths.size());
            Assert.assertTrue(recursivelistHDFSPaths.contains(new Path(listHDFSPath, "newFile1").toString()));
            Assert.assertTrue(recursivelistHDFSPaths.contains(new Path(listHDFSPath, "newFile3").toString()));
            Assert.assertTrue(recursivelistHDFSPaths.contains(new Path(listHDFSPath, "newFile2").toString()));
            Assert.assertTrue(recursivelistHDFSPaths.contains(new Path(listHDFSPath, "folder1/subFile1").toString()));
            Assert.assertTrue(recursivelistHDFSPaths.contains(new Path(listHDFSPath, "folder1/subFile2").toString()));

        fSys.deleteOnExit(new Path("/wrapper"));
        // }
    }

    @Test
    public void testListStatus3() throws IOException {
        // Configuration conf = new HdfsConfiguration();
        // try (MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
        //     String fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();

            conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./wrapper/",
                folder.getRoot().toURI().toString());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./abc/",
                folder.getRoot().toURI().toString() + "/folder1");
            conf.set("fs.hdfs.impl.disable.cache", "true");

            // hdfs native file
            fSys.create(new Path("/hdfsfile1")).close();
            fSys.create(new Path("/hdfsfile2")).close();
            fSys.create(new Path("/hdfsfile3")).close();
            fSys.mkdirs(new Path("/hdfsdir"),
                FsPermission.createImmutable((short) 777));
            fSys.create(new Path("/hdfsdir/subfile1")).close();
            fSys.create(new Path("/hdfsdir/subfile2")).close();

            // wrapped local file system files
            folder.newFile("newFile1");
            folder.newFile("newFile2");
            folder.newFile("newFile3");
            File subfolder = folder.newFolder("folder1");
            subfolder.toPath().resolve("subFile1").toFile().createNewFile();
            subfolder.toPath().resolve("subFile2").toFile().createNewFile();

            FileSystem fs = new Path("hdfs://" + fsAuthority + "/").getFileSystem(conf);
            // Path listPath = new Path("hdfs://" + fsAuthority + "/");
            Path listPathLocal = new Path("hdfs://" + fsAuthority + "/wrapper/");
            Path listPathLocal2 = new Path("hdfs://" + fsAuthority + "/abc/");

            RemoteIterator<FileStatus> filesLocalItr2 = fs.listStatusIterator(listPathLocal2);
            Set<String> filesLocal2Set = remoteItrToList(filesLocalItr2).stream().map(FileStatus::getPath)
                .map(Path::toString).collect(Collectors.toSet());
            Assert.assertEquals(2, filesLocal2Set.size());
            Assert.assertTrue(filesLocal2Set.contains(new Path(listPathLocal2, "subFile1").toString()));
            Assert.assertTrue(filesLocal2Set.contains(new Path(listPathLocal2, "subFile2").toString()));

            RemoteIterator<LocatedFileStatus> locatedFilesLocalItr2 = fs.listLocatedStatus(listPathLocal2);
            Set<String> locatedFilesLocal2Set = remoteItrToList(locatedFilesLocalItr2).stream().map(FileStatus::getPath)
                .map(Path::toString).collect(Collectors.toSet());
            Assert.assertEquals(2, locatedFilesLocal2Set.size());
            Assert.assertTrue(locatedFilesLocal2Set.contains(new Path(listPathLocal2, "subFile1").toString()));
            Assert.assertTrue(locatedFilesLocal2Set.contains(new Path(listPathLocal2, "subFile2").toString()));

            RemoteIterator<LocatedFileStatus> recursiveFilesLocalItr2 = fs.listFiles(listPathLocal2, true);
            Set<String> recursiveFilesLocal2Set = remoteItrToList(recursiveFilesLocalItr2).stream().map(FileStatus::getPath)
                .map(Path::toString).collect(Collectors.toSet());
            Assert.assertEquals(2, recursiveFilesLocal2Set.size());
            Assert.assertTrue(recursiveFilesLocal2Set.contains(new Path(listPathLocal2, "subFile1").toString()));
            Assert.assertTrue(recursiveFilesLocal2Set.contains(new Path(listPathLocal2, "subFile2").toString()));
        fSys.deleteOnExit(new Path("/wrapper"));
        // }
    }

    @Test
    public void testDeleteFile() throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        // Configuration conf = new HdfsConfiguration();
        // try (MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
        //     String fsAuthority = dfsCluster.getNameNode().getClientNamenodeAddress();

            conf.set("fs.hdfs.impl", OBSHDFSFileSystem.class.getName());
            conf.set("fs.trash.interval", "30");
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./wrapper/",
                folder.getRoot().toURI().toString());
            conf.set("fs.hdfs.mounttable." + fsAuthority + ".link./abc/",
                folder.getRoot().toURI().toString() + "/folder1");
            conf.set("fs.hdfs.impl.disable.cache", "true");

            // hdfs native file
            fSys.create(new Path("/hdfsfile1")).close();
            fSys.create(new Path("/hdfsfile2")).close();
            fSys.create(new Path("/hdfsfile3")).close();
            fSys.mkdirs(new Path("/hdfsdir"),
                FsPermission.createImmutable((short) 777));
            fSys.create(new Path("/hdfsdir/subfile1")).close();
            fSys.create(new Path("/hdfsdir/subfile2")).close();

            // wrapped local file system files
            folder.newFile("newFile1");
            folder.newFile("newFile2");
            folder.newFile("newFile3");
            File subfolder = folder.newFolder("folder1");
            subfolder.toPath().resolve("subFile1").toFile().createNewFile();
            subfolder.toPath().resolve("subFile2").toFile().createNewFile();

            FileSystem fs = new Path("hdfs://" + fsAuthority + "/").getFileSystem(conf);
            Path file1 = new Path("hdfs://" + fsAuthority + "/hdfsfile1");
            Assert.assertTrue(fs.delete(file1, false));
            try {
                fs.getFileStatus(file1);
                fail("should throw FileNotFoundException");
            } catch (IOException e) {
                Assert.assertTrue(e instanceof FileNotFoundException);
                Assert.assertFalse(fs.exists(file1));
            }

            Path fileFolder = new Path("hdfs://" + fsAuthority + "/hdfsdir");
            Assert.assertTrue(fs.delete(fileFolder, true));
            Assert.assertFalse(fs.exists(fileFolder));

            Path fileLocal1 = new Path("hdfs://" + fsAuthority + "/wrapper/newFile1");
            Assert.assertTrue(fs.delete(fileLocal1, false));

            try {
                fs.getFileStatus(fileLocal1);
                fail("should throw FileNotFoundException");
            } catch (IOException e) {
                Assert.assertTrue(e instanceof FileNotFoundException);
                Assert.assertFalse(fs.exists(fileLocal1));
            }

            Path fileLocalFolder = new Path("hdfs://" + fsAuthority + "/wrapper/folder1");
            Assert.assertTrue(fs.delete(fileLocalFolder, true));
            Assert.assertFalse(fs.exists(fileLocalFolder));
        // }
    }

    private <T> List<T> remoteItrToList(RemoteIterator<T> itr) throws IOException {
        List<T> list = new ArrayList<>();
        if (itr != null) {
            while (itr.hasNext()) {
                list.add(itr.next());
            }
        }
        return list;
    }

    Random r = new Random();
    private byte[] generateByteArry(int length) {
        byte[] arr = new byte[length];
        r.nextBytes(arr);
        return arr;
    }

    @Override
    public void testFsStatus() {
        skip("Unsupport.");
    }

    @Override
    public void testWorkingDirectory() {
        skip("Unsupport.");
    }

    @Override
    public void testListStatusThrowsExceptionForUnreadableDir() {
        skip("Unsupport.");
    }

    @Override
    public void testRenameDirectoryAsEmptyDirectory() {
        skip("Unsupport.");
    }

    @Override
    public void testRenameDirectoryAsFile() {
        skip("Unsupport.");
    }

    @Override
    public void testRenameDirectoryAsNonEmptyDirectory() {
        skip("Unsupport.");
    }

    @Override
    public void testRenameDirectoryToItself(){
        skip("Unsupport.");
    }

    @Override
    public void testRenameDirectoryToNonExistentParent(){
        skip("Unsupport.");
    }

    @Override
    public void testRenameFileAsExistingDirectory(){
        skip("Unsupport.");
    }

    @Override
    public void testRenameFileAsExistingFile(){
        skip("Unsupport.");
    }

    @Override
    public void testRenameFileToItself(){
        skip("Unsupport.");
    }

    @Override
    public void testRenameFileToNonExistentDirectory(){
        skip("Unsupport.");
    }

    @Override
    public void testRenameNonExistentPath(){
        skip("Unsupport.");
    }

    @Override
    public void testWDAbsolute(){
        skip("Unsupport.");
    }
}

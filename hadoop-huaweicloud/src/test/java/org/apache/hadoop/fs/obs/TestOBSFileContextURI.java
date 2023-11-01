/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.obs;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.apache.hadoop.fs.FileContextURIBase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * <p>
 * A collection of tests for the {@link FileContext} to test path names passed
 * as URIs. This test should be used for testing an instance of FileContext that
 * has been initialized to a specific default FileSystem such a LocalFileSystem,
 * HDFS,OBS, etc, and where path names are passed that are URIs in a different
 * FileSystem.
 * </p>
 *
 * <p>
 * To test a given {@link FileSystem} implementation create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fc1</code> and
 * <code>fc2</code>
 * <p>
 * The tests will do operations on fc1 that use a URI in fc2
 * <p>
 * {@link FileContext} instance variable.
 * </p>
 */
public class TestOBSFileContextURI extends FileContextURIBase {

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        conf.addResource(OBSContract.CONTRACT_XML);
        String fileSystem = conf.get(OBSTestConstants.TEST_FS_OBS_NAME);
        if (fileSystem == null || fileSystem.trim().length() == 0) {
            throw new Exception("Default file system not configured.");
        }

        URI uri = new URI(fileSystem);
        FileSystem fs = OBSTestUtils.createTestFileSystem(conf);
        fc1 = FileContext.getFileContext(new DelegateToFileSystem(uri, fs,
            conf, fs.getScheme(), false) {
        }, conf);

        fc2 = FileContext.getFileContext(new DelegateToFileSystem(uri, fs,
            conf, fs.getScheme(), false) {
        }, conf);
        super.setUp();
    }

    @Override
    public void testFileStatus() {
        skip("Unsupport.");
    }


    @Test
    @Override
    public void testCreateFile() throws IOException {
        String[] fileNames = new String[]{"testFile", "test File", "test*File", "test#File", "test1234", "1234Test", "test)File", "test_File", "()&^%$#@!~_+}{><?", "  ", "^ "};
        String[] arr$ = fileNames;
        int len$ = fileNames.length;

        for(int i$ = 0; i$ < len$; ++i$) {
            String f = arr$[i$];
                Path testPath = this.qualifiedPath(
                    f.replaceAll("%(?![0-9a-fA-F]{2})", "%25"), this.fc2);
                Assert.assertFalse(
                    FileContextTestHelper.exists(this.fc2, testPath));
                FileContextTestHelper.createFile(this.fc1, testPath);
                Assert.assertTrue(FileContextTestHelper.exists(this.fc2, testPath));
        }
    }

    @Test
    @Override
    public void testCreateDirectory() throws IOException {
        Path path = this.qualifiedPath("test/hadoop", this.fc2);
        Path falsePath = this.qualifiedPath("path/doesnot.exist", this.fc2);
        Path subDirPath = this.qualifiedPath("dir0", this.fc2);
        Assert.assertFalse(FileContextTestHelper.exists(this.fc1, path));
        Assert.assertFalse(FileContextTestHelper.isFile(this.fc1, path));
        Assert.assertFalse(FileContextTestHelper.isDir(this.fc1, path));
        this.fc1.mkdir(path, FsPermission.getDefault(), true);
        Assert.assertTrue(FileContextTestHelper.isDir(this.fc2, path));
        Assert.assertTrue(FileContextTestHelper.exists(this.fc2, path));
        Assert.assertFalse(FileContextTestHelper.isFile(this.fc2, path));
        this.fc1.mkdir(subDirPath, FsPermission.getDefault(), true);
        this.fc1.mkdir(subDirPath, FsPermission.getDefault(), true);
        this.fc1.mkdir(subDirPath, FsPermission.getDefault(), true);
        Path parentDir = path.getParent();
        Assert.assertTrue(FileContextTestHelper.exists(this.fc2, parentDir));
        Assert.assertFalse(FileContextTestHelper.isFile(this.fc2, parentDir));
        Path grandparentDir = parentDir.getParent();
        Assert.assertTrue(FileContextTestHelper.exists(this.fc2, grandparentDir));
        Assert.assertFalse(FileContextTestHelper.isFile(this.fc2, grandparentDir));
        Assert.assertFalse(FileContextTestHelper.exists(this.fc2, falsePath));
        Assert.assertFalse(FileContextTestHelper.isDir(this.fc2, falsePath));
        String[] dirNames = new String[]{"createTest/testDir", "createTest/test Dir", "deleteTest/test*Dir", "deleteTest/test#Dir", "deleteTest/test1234", "deleteTest/test_DIr", "deleteTest/1234Test", "deleteTest/test)Dir", "deleteTest/()&^%$#@!~_+}{><?", "  ", "^ "};
        String[] arr$ = dirNames;
        int len$ = dirNames.length;

        for(int i$ = 0; i$ < len$; ++i$) {
            String f = arr$[i$];
                Path testPath = this.qualifiedPath(
                    f.replaceAll("%(?![0-9a-fA-F]{2})", "%25"), this.fc2);
                Assert.assertFalse(FileContextTestHelper.exists(this.fc2, testPath));
                this.fc1.mkdir(testPath, FsPermission.getDefault(), true);
                Assert.assertTrue(FileContextTestHelper.exists(this.fc2, testPath));
                Assert.assertTrue(FileContextTestHelper.isDir(this.fc2, testPath));
        }
    }


    @Test
    @Override
    public void testDeleteDirectory() throws IOException {
        String dirName = "dirTest";
        Path testDirPath = this.qualifiedPath(dirName, this.fc2);
        Assert.assertFalse(FileContextTestHelper.exists(this.fc2, testDirPath));
        this.fc1.mkdir(testDirPath, FsPermission.getDefault(), true);
        Assert.assertTrue(FileContextTestHelper.exists(this.fc2, testDirPath));
        Assert.assertTrue(FileContextTestHelper.isDir(this.fc2, testDirPath));
        this.fc2.delete(testDirPath, true);
        Assert.assertFalse(FileContextTestHelper.isDir(this.fc2, testDirPath));
        String[] dirNames = new String[]{"deleteTest/testDir", "deleteTest/test Dir", "deleteTest/test*Dir", "deleteTest/test#Dir", "deleteTest/test1234", "deleteTest/1234Test", "deleteTest/test)Dir", "deleteTest/test_DIr", "deleteTest/()&^%$#@!~_+}{><?", "  ", "^ "};
        String[] arr$ = dirNames;
        int len$ = dirNames.length;

        for(int i$ = 0; i$ < len$; ++i$) {
            String f = arr$[i$];
                Path testPath = this.qualifiedPath(
                    f.replaceAll("%(?![0-9a-fA-F]{2})", "%25"), this.fc2);
                Assert.assertFalse(FileContextTestHelper.exists(this.fc2, testPath));
                this.fc1.mkdir(testPath, FsPermission.getDefault(), true);
                Assert.assertTrue(FileContextTestHelper.exists(this.fc2, testPath));
                Assert.assertTrue(FileContextTestHelper.isDir(this.fc2, testPath));
                Assert.assertTrue(this.fc2.delete(testPath, true));
                Assert.assertFalse(FileContextTestHelper.exists(this.fc2, testPath));
                Assert.assertFalse(FileContextTestHelper.isDir(this.fc2, testPath));
        }
    }

    @AfterClass
    public static void clearBucket() throws IOException {
        OBSFSTestUtil.clearBucket();
    }
}

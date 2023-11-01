/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.fs.obs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * This class tests the FileStatus API.
 */
public class ITestListFiles {
    static final long seed = 0xDEADBEEFL;

    private OBSFileSystem fs;

    private Configuration conf;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private static Path TEST_DIR;

    final private static int FILE_LEN = 10;

    private static Path FILE1;

    private static Path DIR1;

    private static Path FILE2;

    private static Path FILE3;

    static {
        setTestPaths(new Path(testRootPath, "main_"));
    }

    /**
     * Sets the root testing directory and reinitializes any additional test paths
     * that are under the root.  This method is intended to be called from a
     * subclass's @BeforeClass method if there is a need to override the testing
     * directory.
     *
     * @param testDir Path root testing directory
     */
    protected static void setTestPaths(Path testDir) {
        TEST_DIR = testDir;
        FILE1 = new Path(TEST_DIR, "file1");
        DIR1 = new Path(TEST_DIR, "dir1");
        FILE2 = new Path(DIR1, "file2");
        FILE3 = new Path(DIR1, "file3");
    }

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        conf = OBSContract.getConfiguration(null);
        conf.setClass(OBSConstants.OBS_METRICS_CONSUMER,
            MockMetricsConsumer.class, BasicMetricsConsumer.class);
        conf.setBoolean(OBSConstants.METRICS_SWITCH, true);
        fs = OBSTestUtils.createTestFileSystem(conf);
        fs.delete(TEST_DIR, true);
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
            fs.close();
            fs = null;
        }
    }

    private Path getTestPath(String testPath) {
        return new Path(testRootPath + "/" + testPath);
    }

    private static void writeFile(FileSystem fileSys, Path name, int fileSize)
        throws IOException {
        // Create and write a file that contains three blocks of data
        FSDataOutputStream stm = fileSys.create(name);
        byte[] buffer = new byte[fileSize];
        Random rand = new Random(seed);
        rand.nextBytes(buffer);
        stm.write(buffer);
        stm.close();
    }

    /**
     * Test when input path is a file
     */
    @Test
    public void testFile() throws IOException {
        fs.mkdirs(TEST_DIR);
        writeFile(fs, FILE1, FILE_LEN);

        RemoteIterator<LocatedFileStatus> itor = fs.listFiles(
            FILE1, true);
        LocatedFileStatus stat = itor.next();
        assertFalse(itor.hasNext());
        assertTrue(stat.isFile());
        assertEquals(FILE_LEN, stat.getLen());
        assertEquals(fs.makeQualified(FILE1), stat.getPath());
        assertEquals(1, stat.getBlockLocations().length);

        itor = fs.listFiles(FILE1, false);
        stat = itor.next();
        assertFalse(itor.hasNext());
        assertTrue(stat.isFile());
        assertEquals(FILE_LEN, stat.getLen());
        assertEquals(fs.makeQualified(FILE1), stat.getPath());
        assertEquals(1, stat.getBlockLocations().length);

        fs.delete(FILE1, true);
    }

    /**
     * Test when input path is a directory
     */
    @Test
    public void testDirectory() throws IOException {
        fs.mkdirs(DIR1);

        // test empty directory
        RemoteIterator<LocatedFileStatus> itor = fs.listFiles(
            DIR1, true);
        assertFalse(itor.hasNext());
        itor = fs.listFiles(DIR1, false);
        assertFalse(itor.hasNext());

        // testing directory with 1 file
        writeFile(fs, FILE2, FILE_LEN);
        itor = fs.listFiles(DIR1, true);
        LocatedFileStatus stat = itor.next();
        assertFalse(itor.hasNext());
        assertTrue(stat.isFile());
        assertEquals(FILE_LEN, stat.getLen());
        assertEquals(fs.makeQualified(FILE2), stat.getPath());
        assertEquals(1, stat.getBlockLocations().length);

        itor = fs.listFiles(DIR1, false);
        stat = itor.next();
        assertFalse(itor.hasNext());
        assertTrue(stat.isFile());
        assertEquals(FILE_LEN, stat.getLen());
        assertEquals(fs.makeQualified(FILE2), stat.getPath());
        assertEquals(1, stat.getBlockLocations().length);

        // test more complicated directory
        writeFile(fs, FILE1, FILE_LEN);
        writeFile(fs, FILE3, FILE_LEN);

        Set<Path> filesToFind = new HashSet<Path>();
        filesToFind.add(fs.makeQualified(FILE1));
        filesToFind.add(fs.makeQualified(FILE2));
        filesToFind.add(fs.makeQualified(FILE3));

        itor = fs.listFiles(TEST_DIR, true);
        stat = itor.next();
        assertTrue(stat.isFile());
        assertTrue("Path " + stat.getPath() + " unexpected",
            filesToFind.remove(stat.getPath()));

        stat = itor.next();
        assertTrue(stat.isFile());
        assertTrue("Path " + stat.getPath() + " unexpected",
            filesToFind.remove(stat.getPath()));

        stat = itor.next();
        assertTrue(stat.isFile());
        assertTrue("Path " + stat.getPath() + " unexpected",
            filesToFind.remove(stat.getPath()));
        assertFalse(itor.hasNext());
        assertTrue(filesToFind.isEmpty());

        itor = fs.listFiles(TEST_DIR, false);
        stat = itor.next();
        assertTrue(stat.isFile());
        assertEquals(fs.makeQualified(FILE1), stat.getPath());
        assertFalse(itor.hasNext());

        fs.delete(TEST_DIR, true);
    }

    @Test
    // 调用listStatus递归列举接口，recursive为true，返回目录及子目录下所有对象；recursive为false，只返回当前目录下对象
    public void testListStatusRecursive() throws Exception {
        Path testDir = getTestPath("test_dir/");
        Path subDir = getTestPath("test_dir/sub_dir/");
        Path file1 = getTestPath("test_dir/file1");
        Path file2 = getTestPath("test_dir/sub_dir/file2");
        fs.mkdirs(testDir);
        fs.mkdirs(subDir);
        FSDataOutputStream outputStream = fs.create(file1, false);
        outputStream.close();
        outputStream = fs.create(file2, false);
        outputStream.close();

        FileStatus[] objects = fs.listStatus(testDir, false);
        assertEquals(2, objects.length);

        objects = fs.listStatus(testDir, true);
        assertEquals(3, objects.length);
        fs.delete(testDir, true);
    }
}

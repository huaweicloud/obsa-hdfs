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

import com.obs.services.exception.ObsException;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.fs.obs.OBSUtils.translateException;

/**
 *  Tests a live OBS system. If your keys and bucket aren't specified, all tests
 *  are marked as passed.
 *
 *  This uses BlockJUnit4ClassRunner because FileSystemContractBaseTest from
 *  TestCase which uses the old Junit3 runner that doesn't ignore assumptions
 *  properly making it impossible to skip the tests if we don't have a valid
 *  bucket.
 **/
public class ITestOBSFileSystemContract extends FileSystemContractBaseTest {

  protected static final Logger LOG =
          LoggerFactory.getLogger(ITestOBSFileSystemContract.class);

  private Path basePath;

  private OBSFileSystem obsFS;

  @Rule
  public TestName methodName = new TestName();

  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit-" + methodName.getMethodName());
  }

  @Override
  public void setUp() throws Exception {
    Configuration conf = new Configuration();

    obsFS = OBSTestUtils.createTestFileSystem(conf);
    fs = obsFS;
    basePath = fs.makeQualified(
            OBSTestUtils.createTestPath(new Path("/obsfilesystemcontract")));
    super.setUp();
  }

  /**
   * This path explicitly places all absolute paths under the per-test suite
   * path directory; this allows the test to run in parallel.
   * @param pathString path string as input
   * @return a qualified path string.
   */
  protected Path path(String pathString) {
    if (pathString.startsWith("/")) {
      return fs.makeQualified(new Path(basePath, pathString));
    } else {
      return super.path(pathString);
    }
  }

  @Override
  protected void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(basePath, true);
    }
    super.tearDown();
  }

  @Override
  public void testMkdirsWithUmask() throws Exception {
    // not supported
  }

  @Override
  public void testRenameDirectoryAsExistingDirectory() throws Exception {
    if (!renameSupported()) {
      return;
    }

    // Prepare the source folder with some nested files or sub folders.
    Path src = path("/test/hadoop/dir");
    fs.mkdirs(src);

    createFile(path("/test/hadoop/dir/file1"));
    createFile(path("/test/hadoop/dir/subdir1/file11"));
    createFile(path("/test/hadoop/dir/subdir2/file21"));
    createFile(path("/test/hadoop/dir/subdir1/subdir11/file111"));
    createFile(path("/test/hadoop/dir/file2"));
    fs.mkdirs( path("/test/hadoop/dir/subdir3/subdir31"));

    // Prepare the destination folder.
    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst);

    // Execute the rename operation.
    rename(src, dst, true, false, true);

    // Assert that all nested files or sub folders under the source folder should be not existed after rename.
    assertFalse("Nested file1 exists",
            fs.exists(path("/test/hadoop/dir/file1")));
    assertFalse("Nested file2 exists",
            fs.exists(path("/test/hadoop/dir/file2")));
    assertFalse("Nested file11 exists",
            fs.exists(path("/test/hadoop/dir/subdir1/file11")));
    assertFalse("Nested file111 exists",
            fs.exists(path("/test/hadoop/dir/subdir1/subdir11/file111")));
    assertFalse("Nested file111 exists",
            fs.exists(path("/test/hadoop/dir/subdir1/subdir11")));
    assertFalse("Nested file111 exists",
            fs.exists(path("/test/hadoop/dir/subdir1")));
    assertFalse("Nested file21 exists",
            fs.exists(path("/test/hadoop/dir/subdir2/file21")));
    assertFalse("Nested file21 exists",
            fs.exists(path("/test/hadoop/dir/subdir2")));
    assertFalse("Nested subdir31 exists",
            fs.exists(path("/test/hadoop/dir/subdir3/subdir31")));
    assertFalse("Nested subdir31 exists",
            fs.exists(path("/test/hadoop/dir/subdir3")));
    assertFalse("Nested subdir31 exists",
            fs.exists(path("/test/hadoop/dir")));

    // Assert that all nested files or sub folders should be under the destination folder after rename.
    assertTrue("Renamed nested file1 exists",
            fs.exists(path("/test/new/newdir/file1")));
    assertTrue("Renamed nested file2 exists",
            fs.exists(path("/test/new/newdir/file2")));
    assertTrue("Renamed nested file11 exists",
            fs.exists(path("/test/new/newdir/subdir1")));
    assertTrue("Renamed nested file11 exists",
            fs.exists(path("/test/new/newdir/subdir1/file11")));
    assertTrue("Renamed nested file111 exists",
            fs.exists(path("/test/new/newdir/subdir1/subdir11")));
    assertTrue("Renamed nested file111 exists",
            fs.exists(path("/test/new/newdir/subdir1/subdir11/file111")));
    assertTrue("Renamed nested file21 exists",
            fs.exists(path("/test/new/newdir/subdir2")));
    assertTrue("Renamed nested file21 exists",
            fs.exists(path("/test/new/newdir/subdir2/file21")));
    assertTrue("Renamed nested file21 exists",
            fs.exists(path("/test/new/newdir/subdir3")));
    assertTrue("Renamed nested subdir31 exists",
            fs.exists(path("/test/new/newdir/subdir3/subdir31")));
  }

  // todo : to delete this case that has been existed in base class FileSystemContractBaseTest.
  @Override
  public void testRenameDirectoryMoveToExistingDirectory() throws Exception {
    if (this.renameSupported()) {
      Path src = this.path("/test/hadoop/dir");
      this.fs.mkdirs(src);
      this.createFile(this.path("/test/hadoop/dir/file1"));
      this.createFile(this.path("/test/hadoop/dir/subdir/file2"));
      Path dst = this.path("/test/new/newdir");
      this.fs.mkdirs(dst.getParent());
      this.rename(src, dst, true, false, true);
      /* todo : temporarily avoid the problem that the file-gateway doesn't broadcast the metadata updation to OSC metadata cache.
      assertFalse("Nested file1 exists", this.fs.exists(this.path("/test/hadoop/dir/file1")));
      assertFalse("Nested file2 exists", this.fs.exists(this.path("/test/hadoop/dir/subdir/file2")));
      assertFalse("Nested file2 exists", this.fs.exists(this.path("/test/hadoop/dir/subdir")));
      */
      assertFalse("Nested file2 exists", this.fs.exists(this.path("/test/hadoop/dir")));
      assertTrue("Renamed nested newdir exists", this.fs.exists(this.path("/test/new/newdir")));
      assertTrue("Renamed nested file1 exists", this.fs.exists(this.path("/test/new/newdir/file1")));
      assertTrue("Renamed nested subdir exists", this.fs.exists(this.path("/test/new/newdir/subdir")));
      assertTrue("Renamed nested file2 exists", this.fs.exists(this.path("/test/new/newdir/subdir/file2")));
    }
  }

  //  @Override
  public void testMoveDirUnderParent() throws Throwable {
    // not support because
    // Fails if dst is a directory that is not empty.
  }

  public void testRecursivelyDeleteDirectory() throws Exception {

    // Prepare the source folder with some nested files or sub folders.
    Path p_dir = path("/test/hadoop/dir");
    fs.mkdirs(p_dir);

    createFile(path("/test/hadoop/dir/FILE_0"));
    createFile(path("/test/hadoop/dir/SDIR_0/FILE_00"));
    createFile(path("/test/hadoop/dir/SDIR_0/SDIR_00/FILE_000"));
    fs.mkdirs( path("/test/hadoop/dir/SDIR_0/SDIR_00/SDIR_000"));
    createFile(path("/test/hadoop/dir/SDIR_0/SDIR_00/file_001"));
    fs.mkdirs( path("/test/hadoop/dir/SDIR_0/SDIR_00/sdir_001"));
    createFile(path("/test/hadoop/dir/SDIR_0/file_01"));
    createFile(path("/test/hadoop/dir/SDIR_0/sdir_01/FILE_010"));
    fs.mkdirs( path("/test/hadoop/dir/SDIR_0/sdir_01/SDIR_010"));
    createFile(path("/test/hadoop/dir/SDIR_0/sdir_01/file_011"));
    fs.mkdirs( path("/test/hadoop/dir/SDIR_0/sdir_01/sdir_011"));
    createFile(path("/test/hadoop/dir/file_1"));
    createFile(path("/test/hadoop/dir/sdir_1/FILE_10"));
    createFile(path("/test/hadoop/dir/sdir_1/SDIR_10/FILE_100"));
    fs.mkdirs( path("/test/hadoop/dir/sdir_1/SDIR_10/SDIR_100"));
    createFile(path("/test/hadoop/dir/sdir_1/SDIR_10/file_101"));
    fs.mkdirs( path("/test/hadoop/dir/sdir_1/SDIR_10/sdir_101"));
    createFile(path("/test/hadoop/dir/sdir_1/file_11"));
    createFile(path("/test/hadoop/dir/sdir_1/sdir_11/FILE_110"));
    fs.mkdirs( path("/test/hadoop/dir/sdir_1/sdir_11/SDIR_110"));
    createFile(path("/test/hadoop/dir/sdir_1/sdir_11/file_111"));
    fs.mkdirs( path("/test/hadoop/dir/sdir_1/sdir_11/sdir_111"));

    // Assert that all nested files or sub folders under the source folder should be existed before delete.
    assertTrue("created", fs.exists(path("/test")));
    assertTrue("created", fs.exists(path("/test/hadoop")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/FILE_0")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/SDIR_0")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/SDIR_0/FILE_00")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/SDIR_0/SDIR_00")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/SDIR_0/SDIR_00/FILE_000")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/SDIR_0/SDIR_00/SDIR_000")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/SDIR_0/SDIR_00/file_001")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/SDIR_0/SDIR_00/sdir_001")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/SDIR_0/file_01")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/SDIR_0/sdir_01")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/SDIR_0/sdir_01/FILE_010")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/SDIR_0/sdir_01/SDIR_010")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/SDIR_0/sdir_01/file_011")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/SDIR_0/sdir_01/sdir_011")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/file_1")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/sdir_1")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/sdir_1/FILE_10")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/sdir_1/SDIR_10")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/sdir_1/SDIR_10/FILE_100")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/sdir_1/SDIR_10/SDIR_100")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/sdir_1/SDIR_10/file_101")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/sdir_1/SDIR_10/sdir_101")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/sdir_1/file_11")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/sdir_1/sdir_11")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/sdir_1/sdir_11/FILE_110")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/sdir_1/sdir_11/SDIR_110")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/sdir_1/sdir_11/file_111")));
    assertTrue("created", fs.exists(path("/test/hadoop/dir/sdir_1/sdir_11/sdir_111")));

    fs.delete(p_dir, true);

    // Assert that all nested files or sub folders under the source folder should be not existed after delete.
    assertFalse("created", fs.exists(path("/test/hadoop/dir")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/FILE_0")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/SDIR_0")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/SDIR_0/FILE_00")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/SDIR_0/SDIR_00")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/SDIR_0/SDIR_00/FILE_000")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/SDIR_0/SDIR_00/SDIR_000")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/SDIR_0/SDIR_00/file_001")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/SDIR_0/SDIR_00/sdir_001")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/SDIR_0/file_01")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/SDIR_0/sdir_01")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/SDIR_0/sdir_01/FILE_010")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/SDIR_0/sdir_01/SDIR_010")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/SDIR_0/sdir_01/file_011")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/SDIR_0/sdir_01/sdir_011")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/file_1")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/sdir_1")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/sdir_1/FILE_10")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/sdir_1/SDIR_10")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/sdir_1/SDIR_10/FILE_100")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/sdir_1/SDIR_10/SDIR_100")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/sdir_1/SDIR_10/file_101")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/sdir_1/SDIR_10/sdir_101")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/sdir_1/file_11")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/sdir_1/sdir_11")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/sdir_1/sdir_11/FILE_110")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/sdir_1/sdir_11/SDIR_110")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/sdir_1/sdir_11/file_111")));
    assertFalse("created", fs.exists(path("/test/hadoop/dir/sdir_1/sdir_11/sdir_111")));
    assertTrue("created", fs.exists(path("/test")));
    assertTrue("created", fs.exists(path("/test/hadoop")));

    return;
  }

  private long getDigitsNumber(long num) {
    long num1 = num - 1;
    long len = 0;
    while (num1 > 0) {
      len++;
      num1 = num1 / 10;
    }
    return (len == 0 ? 1 : len);
  }

  private long prepareObjectsTree(String parent, int[] arWidth, String[] arNamePrefix, boolean isFile)
          throws Exception {
    // Check the input parameters.
    int depth = arWidth.length;
    if (arNamePrefix.length != depth) {
      throw new Exception("Unmatched depth.");
    }

    String digitFormats[] = new String[depth];
    for (int dep = 0; dep < depth; dep++) {
      if (arWidth[dep] <= 0) {
        throw new Exception("The width of any level must be positive.");
      }
      digitFormats[dep] = String.format("%%0%dd", getDigitsNumber(arWidth[dep]));
    }

    String p_key = (!parent.endsWith("/") ? parent : parent.substring(0, parent.length() - 1));

    long totalNum = 0;
    long leavesNum = 1;
    long factors[] = new long[depth];
    for (int dep = 0; dep < depth; dep++) {
      if (dep == 0) {
        factors[depth - 1] = 1;
      } else {
        factors[depth - dep - 1] = factors[depth - dep] * arWidth[depth - dep];
      }
      leavesNum = leavesNum * arWidth[dep];
      totalNum += leavesNum;
    }

    for (long seq = 0; seq < leavesNum; seq++) {
      String seqStr = p_key;
      String digitStr = "";
      long seqTmp = seq;
      for (int dep = 0; dep < depth; dep++) {
        long fac = factors[dep];
        digitStr = digitStr + "_" + String.format(digitFormats[dep], seqTmp / fac);
        seqTmp = seqTmp % fac;
        seqStr = seqStr + "/" + arNamePrefix[dep] + digitStr;
      }
      if (isFile) {
        createFile(path(seqStr));
      } else {
        fs.mkdirs(path(seqStr));
      }
    }

    return totalNum;
  }

  private long prepareFilesTree(String parent, int[] arWidth, String[] arNamePrefix)
          throws Exception {
    return prepareObjectsTree(parent, arWidth, arNamePrefix, true);
  }

  private long prepareFoldersTree(String parent, int[] arWidth, String[] arNamePrefix)
          throws Exception {
    return prepareObjectsTree(parent, arWidth, arNamePrefix, false);
  }

  private long prepareObjectsTree(String parent, int objNumOfEachPart)
          throws Exception {
    Path parentPath = path(parent);
    fs.mkdirs(parentPath);

    long partsNum = 0;
    long totalNum = 0;

    // Prepare files tree.
    int[] arWidth1 = {objNumOfEachPart};
    String[] arPrefix1 = {"file"};
    partsNum = prepareFilesTree(parent, arWidth1, arPrefix1);
    assertEquals(objNumOfEachPart, partsNum);
    totalNum += partsNum;

    int[] arWidth2 = {objNumOfEachPart};
    String[] arPrefix2 = {"FILE"};
    partsNum = prepareFilesTree(parent, arWidth2, arPrefix2);
    assertEquals(objNumOfEachPart, partsNum);
    totalNum += partsNum;

    int[] arWidth3 = {objNumOfEachPart, 2};
    String[] arPrefix3 = {"sdir", "FILE"};
    partsNum = prepareFilesTree(parent, arWidth3, arPrefix3);
    assertEquals(objNumOfEachPart * 3, partsNum);
    totalNum += partsNum;

    int[] arWidth4 = {objNumOfEachPart, 2};
    String[] arPrefix4 = {"SDIR", "file"};
    partsNum = prepareFilesTree(parent, arWidth4, arPrefix4);
    assertEquals(objNumOfEachPart * 3, partsNum);
    totalNum += partsNum;

    return totalNum;
  }

  public class ObjectClusterDescription {
    boolean isFile;
    String prefix;
    int width;
    String seqFmt;

    public ObjectClusterDescription(boolean isFile, String prefix, int width) {
      this.isFile = isFile;
      this.prefix = prefix;
      this.width = width;
      if (this.width > 0) {
        this.seqFmt = String.format("%%0%dd", getDigitsNumber(this.width));
      } else {
        this.seqFmt = null;
      }
    }

    public void Statistic(ObjectTreeInfo info) {
      if (this.isFile) {
        info.filesNum += this.width;
      } else {
        info.foldersNum += this.width;
      }
    }
  }

  public class ObjectTreeInfo {
    public int filesNum = 0;
    public int foldersNum = 0;
    public int leafFoldersNum = 0;

    public ObjectTreeInfo() { reset(); }

    public ObjectTreeInfo(int filesNum, int foldersNum, int leafFoldersNum) {
      this.filesNum = filesNum;
      this.foldersNum = foldersNum;
      this.leafFoldersNum = leafFoldersNum;
    }

    public boolean isEmtpy() {
      return ((this.filesNum == 0) && (this.foldersNum == 0));
    }

    public void reset() {
      filesNum = 0;
      foldersNum = 0;
      leafFoldersNum = 0;
    }
  }

  private ObjectTreeInfo getObjectTreeInfoOfOneDepth(ObjectClusterDescription[] arDesc) {
    ObjectTreeInfo info = new ObjectTreeInfo();
    for (int i = 0; i < arDesc.length; i++) {
      if (arDesc[i] == null) {
        continue;
      }
      arDesc[i].Statistic(info);
    }
    return info;
  }

  // prepare sub objects of one cluster that described by one object cluster description.
  private void prepareObjectsOfOneCluster(ObjectClusterDescription desc,
                                          String parent,
                                          boolean atDeepestDepth,
                                          List<String> leaves,
                                          List<String> currFolders) {
    if (desc == null) {
      return;
    }
    for (int seq = 0; seq < desc.width; seq++) {
      String key = parent + desc.prefix + "_" + String.format(desc.seqFmt, seq);
      if (desc.isFile) {
        // file must be leaf.
        leaves.add(key);
        continue;
      }
      // folder
      key += "/";
      if (atDeepestDepth) {
        // folder at deepest depth is leaf.
        leaves.add(key);
      }
      currFolders.add(key);
    }
  }

  // prepare sub objects of clusters that described by an array of object clusters description.
  private void prepareSonObjectsOfOneFolder(ObjectClusterDescription[] arDesc,
                                            String parentFolder,
                                            boolean deepest,
                                            List<String> leaves,
                                            List<String> currFolders) {
    for (int clusterIdx = 0; clusterIdx < arDesc.length; clusterIdx++) {
      ObjectClusterDescription desc = arDesc[clusterIdx];
      prepareObjectsOfOneCluster(desc, parentFolder, deepest, leaves, currFolders);
    }
  }

  private void createLeafObjects(List<String> leaves) throws IOException {
    for (int i = 0; i < leaves.size(); i++) {
      String key = leaves.get(i);
      if (key.endsWith("/")) {
        // Create leaf folder.
        fs.mkdirs(path(key.substring(0,key.length()-1)));
      } else {
        // Create file.
        createFile(path(key));
      }
    }
  }

  private ObjectTreeInfo getObjectTreeInfo(
          ObjectClusterDescription[][] arDescription) {
    int filesNum = 0;
    int foldersNum = 0;
    int foldersNumAtPreviousDepth = 1;
    int depth = arDescription.length;
    assertTrue(depth > 0);

    for (int currDepth = 0; currDepth < depth; currDepth++) {
      ObjectClusterDescription[] desc = arDescription[currDepth];

      // Statistic the number of files and folders at current depth of a parent folder at previous depth.
      ObjectTreeInfo info = getObjectTreeInfoOfOneDepth(desc);
      assertTrue(!info.isEmtpy());

      // Statistic the number of files at current depth of all parent folders at previous depth.
      info.filesNum *= foldersNumAtPreviousDepth;
      filesNum += info.filesNum;

      // Statistic the number of folders at current depth of all parent folders at previous depth.
      info.foldersNum *= foldersNumAtPreviousDepth;
      foldersNum += info.foldersNum;

      // Assert that the depth that has no folder must be the deepest one.
      assertTrue((info.foldersNum > 0) || ((currDepth + 1) == depth));

      // Save the number of folders at current depth and and the deepest folders are leaves.
      foldersNumAtPreviousDepth = info.foldersNum;
    }

    return new ObjectTreeInfo(filesNum, foldersNum, foldersNumAtPreviousDepth);
  }

  public long prepareObjectsTree(String parent,
                                 ObjectClusterDescription[][] arDescription,
                                 List<String> leaves)
          throws Exception {
    Path parentPath = path(parent);
    fs.mkdirs(parentPath);
    parent = (parent.endsWith("/") ? parent : (parent + "/"));

    // Statistic the number of sub objects of parent.
    ObjectTreeInfo info = getObjectTreeInfo(arDescription);

    // allocate two lists to save folders names at two adjacent depth.
    int leavesNum = info.filesNum + info.leafFoldersNum;
    List<String> prevFolders = null;
    List<String> currFolders = new ArrayList<String>(1);
    currFolders.add(parent);

    // prepare sub objects of each depth one by one.
    int depth = arDescription.length;
    for (int currDepth = 0; currDepth < depth; currDepth++) {
      boolean deepest = ((currDepth + 1) == depth);

      // Swap the two lists.
      prevFolders = currFolders;
      currFolders = new ArrayList<String>(prevFolders.size());

      // prepare sub objects at current depth.
      ObjectClusterDescription[] arDesc = arDescription[currDepth];
      for (int i = 0; i < prevFolders.size(); i++) {
        // prepare sub objects of current parent folder.
        prepareSonObjectsOfOneFolder(arDesc, prevFolders.get(i), deepest, leaves, currFolders);
      }

      if (currFolders.size() == 0) {
        // Current depth has no folder, so it must be deepest one.
        break;
      }
    }
    assertEquals(leavesNum, leaves.size());

    // Create all leaf objects.
    createLeafObjects(leaves);

    // Check that each node in each path of leaves should be existed after preparation.
    assertPathStatus(parent, leaves, true);

    return (info.filesNum + info.foldersNum);
  }

  private void assertDepth(FileStatus[] arFileStatus) {
    int currDepth = Integer.MAX_VALUE;
    for (int i=arFileStatus.length-1; i >= 0; i--) {
      int prevDepth = currDepth;
      currDepth = arFileStatus[i].getPath().depth();
      if (currDepth > prevDepth) {
        assertTrue(false);
      }
    }
  }

  private void assertObjectStatus(Path f, boolean isExisted) {
    try {
      fs.getFileStatus(f);
      assertTrue(isExisted);
    } catch (FileNotFoundException e) {
      assertFalse(isExisted);
    } catch (IOException e) {
      fail(e.toString());
    }
  }

  private void assertPathStatus(String parent, String key, boolean isExisted) {
    assertTrue(key.startsWith(parent));
    Path parentPath = path(parent);
    Path currPath = path(key);
    while (!currPath.equals(parentPath)) {
      assertObjectStatus(currPath, isExisted);
      currPath = currPath.getParent();
      assertFalse(currPath.isRoot());
    }
  }

  private void assertPathStatus(String parent, List<String> leaves, boolean isExisted) {
    for (int i=0; i<leaves.size(); i++) {
      String key = leaves.get(i);
      assertPathStatus(parent, key, isExisted);
    }
    assertObjectStatus(path(parent), isExisted);
  }

//  public void testListStatus() throws Exception {
//    String p_dir1 = "test/dir1/";
//    String p_dir2 = "test/dir2/";
//    Path path1 = path(p_dir1);
//    Path path2 = path(p_dir2);
//
//    fs.mkdirs(path1);
//
//    ObjectClusterDescription[][] arDesc = new ObjectClusterDescription[3][4];
//    arDesc[0][0] = new ObjectClusterDescription(true, "file", 5);
//    arDesc[0][1] = new ObjectClusterDescription(false, "sdir", 5);
//    arDesc[0][2] = new ObjectClusterDescription(true, "FILE", 5);
//    arDesc[0][3] = new ObjectClusterDescription(false, "SDIR", 5);
//
//    arDesc[1][0] = new ObjectClusterDescription(true, "file", 5);
//    arDesc[1][1] = new ObjectClusterDescription(false, "sdir", 5);
//    arDesc[1][2] = new ObjectClusterDescription(true, "FILE", 5);
//    arDesc[1][3] = new ObjectClusterDescription(false, "SDIR", 5);
//
//    arDesc[2][0] = new ObjectClusterDescription(true, "file", 1);
//    arDesc[2][2] = new ObjectClusterDescription(false, "SDIR", 1);
//
//    List<String> leaves = new ArrayList<String>(1);
//    long totalNum = prepareObjectsTree(p_dir1, arDesc, leaves);
//
//    // rename non-empty folder.
//    rename(path1, path2, true, false, true);
//
//    // Check that each node in each path of leaves should be existed after rename.
//    List<String> newLeaves = new ArrayList<String>(leaves.size());
//    for (String key : leaves) {
//      String newKey = key.replaceFirst(p_dir1, p_dir2);
//      newLeaves.add(newKey);
//    }
//    assertPathStatus(p_dir2, newLeaves, true);
//
//    // List sub objects after rename and before delete.
//    FileStatus[] arFileStatus = obsFS.innerListStatus(path2, true);
//    assertEquals(totalNum, arFileStatus.length);
//    assertDepth(arFileStatus);
//
//    // delete non-empty folder.
//    fs.delete(path2, true);
//
//    // Check that each node in each path of leaves should be not existed after delete.
//    assertPathStatus(p_dir2, newLeaves, false);
//
//    return;
//  }

}

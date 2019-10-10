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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.junit.Assert.assertEquals;

/**
 * OBS tests for configuring block size.
 */
public class ITestHFlush {
  private FileSystem fs;
  private static String testRootPath =
          OBSTestUtils.generateUniqueTestPath();
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestHFlush.class);

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.obs.multipart.size", String.valueOf(5 * 1024 * 1024));
    fs = OBSTestUtils.createTestFileSystem(conf);
    if (fs.exists(new Path("testFlush")))
    {
      fs.delete(new Path("testFlush"));
    }
  }
  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(new Path(testRootPath), true);
    }
  }
  private Path getTestPath(String testPath) {
    return new Path(fs.getUri()+testRootPath + testPath);
  }
  @Test
  public void testFlush01()
  {
    try
    {
      doTheJob("testFlush", AppendTestUtil.BLOCK_SIZE, (short)2, false,
              EnumSet.noneOf(HdfsDataOutputStream.SyncFlag.class), 1024 * 1024 * 200);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  @Test
  public void testFlush02()
  {
    try
    {
      doTheJob("testFlush", AppendTestUtil.BLOCK_SIZE, (short)2, false,
              EnumSet.noneOf(HdfsDataOutputStream.SyncFlag.class), 1024 * 1024 * 20);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  @Test
  public void testFlush03()
  {
    try
    {
      doTheJob("testFlush", AppendTestUtil.BLOCK_SIZE, (short)2, false,
              EnumSet.noneOf(HdfsDataOutputStream.SyncFlag.class), 1024 * 1024 * 51);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  /**
   * The method starts new cluster with defined Configuration; creates a file
   * with specified block_size and writes 10 equal sections in it; it also calls
   * hflush/hsync after each write and throws an IOException in case of an error.
   *
   * @param fileName of the file to be created and processed as required
   * @param block_size value to be used for the file's creation
   * @param replicas is the number of replicas
   * @param isSync hsync or hflush
   * @param syncFlags specify the semantic of the sync/flush
   * @throws IOException in case of any errors
   */
  public void doTheJob(final String fileName,
                              long block_size, short replicas, boolean isSync,
                              EnumSet<HdfsDataOutputStream.SyncFlag> syncFlags, int size) throws IOException {
    byte[] fileContent;
    final int SECTIONS = 10;

    fileContent = AppendTestUtil.initBuffer(size);


    FSDataInputStream is;
    try {
      Path path = new Path(fileName);
      final String pathName = new Path(fs.getWorkingDirectory(), path)
              .toUri().getPath();
      FSDataOutputStream stm = fs.create(path, false, 4096, replicas,
              block_size);
      System.out.println("Created file " + fileName);

      int tenth = size/SECTIONS;
      int rounding = size - tenth * SECTIONS;
      for (int i=0; i<SECTIONS; i++) {
        System.out.println("Writing " + (tenth * i) + " to "
                + (tenth * (i + 1)) + " section to file " + fileName);
        // write to the file

        stm.write(fileContent, tenth * i, tenth);

        // Wait while hflush/hsync pushes all packets through built pipeline
        if (isSync) {
          ((OBSBlockOutputStream)stm.getWrappedStream()).hsync();
        } else {
          ((OBSBlockOutputStream)stm.getWrappedStream()).hflush();
        }

        // Check file length if upd-atelength is required
        if (isSync && syncFlags.contains(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH)) {
          long currentFileLength = fs.getFileStatus(path).getLen();
          assertEquals(
                  "File size doesn't match for hsync/hflush with updating the length",
                  tenth * (i + 1), currentFileLength);
        } else if (isSync && syncFlags.contains(HdfsDataOutputStream.SyncFlag.END_BLOCK)) {

        }
        else
        {
          long currentFileLength = fs.getFileStatus(path).getLen();
          assertEquals(
                  "File size doesn't match for hsync/hflush with updating the length",
                  tenth * (i + 1), currentFileLength);
        }

        byte [] toRead = new byte[tenth];
        byte [] expected = new byte[tenth];
        System.arraycopy(fileContent, tenth * i, expected, 0, tenth);
        // Open the same file for read. Need to create new reader after every write operation(!)
        is = fs.open(path);
        is.seek(tenth * i);
        int readBytes = is.read(toRead, 0, tenth);
        System.out.println("Has read " + readBytes);
        Assert.assertTrue("Should've get more bytes", (readBytes > 0) && (readBytes <= tenth));
        is.close();
        checkData(toRead, 0, readBytes, expected, "Partial verification");
      }
      System.out.println("Writing " + (tenth * SECTIONS) + " to " + (tenth * SECTIONS + rounding) + " section to file " + fileName);
//      stm.write(fileContent, tenth * SECTIONS, rounding);
//      ((OBSBlockOutputStream)stm.getWrappedStream()).hflush();
      stm.write(fileContent, tenth * SECTIONS, rounding);
      stm.close();

      System.out.println("file real length :" + fs.getFileStatus(path).getLen());
      assertEquals("File size doesn't match ", size, fs.getFileStatus(path).getLen());
      AppendTestUtil.checkFullFile(fs, path, fileContent.length, fileContent, "hflush()");
    } finally {
      fs.close();
    }
  }

  void checkData(final byte[] actual, int from, int len,
                        final byte[] expected, String message) {
    for (int idx = 0; idx < len; idx++) {
      assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                      expected[from+idx]+" actual "+actual[idx],
              expected[from+idx], actual[idx]);
      actual[idx] = 0;
    }
  }
}

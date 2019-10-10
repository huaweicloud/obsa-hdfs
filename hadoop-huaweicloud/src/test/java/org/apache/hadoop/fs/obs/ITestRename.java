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
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;

import static org.junit.Assert.assertEquals;

/**
 * OBS tests for configuring block size.
 */
public class ITestRename {
  private FileSystem fs;
  private static String testRootPath =
          OBSTestUtils.generateUniqueTestPath();
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestRename.class);

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.obs.multipart.size", String.valueOf(5 * 1024 * 1024));
    fs = OBSTestUtils.createTestFileSystem(conf);
    if (fs.exists(new Path("testPosixNestedRename")))
    {
      fs.delete(new Path("testPosixNestedRename"));
    }
  }
  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(new Path(testRootPath), true);
    }
  }
   @Test
  public void testPosixNestedRenameFromFileToeExistedFolder()
  {
    try
    {
      Path src = new Path("testPosixNestedRename");

      FSDataOutputStream stream = fs.create(src, true);

      byte[] data = ContractTestUtils.dataset(16, 'a', 26);
      stream.write(data);
      stream.close();

      fs.rename(src, new Path("/user"));

      FsStatus status = fs.getStatus(new Path("/user/testPosixNestedRename"));
      if (status == null) {
        throw new IOException(("rename failed."));
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
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

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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.fileStatsToString;
import static org.junit.Assert.assertEquals;

/**
 * OBS tests for configuring block size.
 */
public class ITestOBSBlockSize {
    private FileSystem fs;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private static final Logger LOG =
        LoggerFactory.getLogger(ITestOBSBlockSize.class);

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        fs = OBSTestUtils.createTestFileSystem(conf);
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }

    private Path getTestPath(String testPath) {
        return new Path(fs.getUri() + testRootPath + testPath);
    }

    @Test
    @SuppressWarnings("deprecation")
    // getDefaultBlockSize返回128M，并且在已存在的fs实例修改conf配置，不会生效
    public void testBlockSize() throws Exception {
        long defaultBlockSize = fs.getDefaultBlockSize();
        assertEquals("incorrect blocksize",
            OBSConstants.DEFAULT_FS_OBS_BLOCK_SIZE, defaultBlockSize);
        long newBlockSize = defaultBlockSize * 2;
        fs.getConf().setLong(OBSConstants.FS_OBS_BLOCK_SIZE, newBlockSize);

        Path dir = getTestPath("/testBlockSize");
        Path file = new Path(dir, "file");
        createFile(fs, file, true, dataset(1024, 'a', 'z' - 'a'));
        FileStatus fileStatus = fs.getFileStatus(file);
        assertEquals(
            "modify configuration in exist fs won't have effect: " + fileStatus,
            defaultBlockSize,
            fileStatus.getBlockSize());

        // check the listing  & assert that the block size is picked up by
        // this route too.
        boolean found = false;
        FileStatus[] listing = fs.listStatus(dir);
        fs.delete(file, false);
        for (FileStatus stat : listing) {
            LOG.info("entry: {}", stat);
            if (file.equals(stat.getPath())) {
                found = true;
                assertEquals(
                    "modify configuration in exist fs won't have effect: "
                        + stat,
                    defaultBlockSize,
                    stat.getBlockSize());
            }
        }
        assertTrue("Did not find " + fileStatsToString(listing, ", "), found);

    }

    @Test
    // 根目录blocksize大小为0
    public void testRootFileStatusBlockSize() throws Throwable {
        FileStatus status = fs.getFileStatus(new Path("/"));
        assertTrue("Invalid root blocksize",
            status.getBlockSize() == 0);
    }

}

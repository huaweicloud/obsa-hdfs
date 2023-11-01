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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.TestFSMainOperationsLocalFileSystem;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

/**
 * <p>
 * A collection of tests for the {@link FileSystem}. This test should be used
 * for testing an instance of FileSystem that has been initialized to a specific
 * default FileSystem such a LocalFileSystem, HDFS,OBS, etc.
 * </p>
 * <p>
 * To test a given {@link FileSystem} implementation create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fSys</code> {@link
 * FileSystem} instance variable.
 * <p>
 * Since this a junit 4 you can also do a single setup before the start of any
 * tests. E.g.
 *
 *
 * </p>
 */
public class TestOBSFSMainOperations extends
    TestFSMainOperationsLocalFileSystem {

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @Override
    @Before
    public void setUp() throws Exception {
        skipTestCheck();
        Configuration conf = OBSContract.getConfiguration(null);
        fSys = OBSTestUtils.createTestFileSystem(conf);
    }

    @Override
    public void testListStatusThrowsExceptionForUnreadableDir() {
        skip("Unsupport.");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (fSys != null) {
            super.tearDown();
        }
    }

    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Test
    @Override
    public void testRenameNonExistentPath() throws Exception {
        skip("Unsupport.");
    }

    @AfterClass
    public static void clearBucket() throws IOException {
        OBSFSTestUtil.clearBucket();
    }
}

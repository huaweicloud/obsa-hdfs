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
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextCreateMkdirBaseTest;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

/**
 * File context create mkdir test cases on obs file system.
 */
public class TestOBSFileContextCreateMkdir extends
    FileContextCreateMkdirBaseTest {

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
        fc = FileContext.getFileContext(new DelegateToFileSystem(uri, fs,
            conf, fs.getScheme(), false) {
        }, conf);
        super.setUp();
    }

    @Override
    protected FileContextTestHelper createFileContextHelper() {
        // On Windows, root directory path is created from local running
        // directory.
        // obs does not support ':' as part of the path which results in
        // failure.
        return new FileContextTestHelper(UUID.randomUUID().toString());
    }

    @AfterClass
    public static void clearBucket() throws IOException {
        OBSFSTestUtil.clearBucket();
    }
}

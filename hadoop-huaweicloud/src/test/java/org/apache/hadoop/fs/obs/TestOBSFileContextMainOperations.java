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
import org.apache.hadoop.fs.FileContextMainOperationsBaseTest;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * Rename test cases on obs file system.
 */
public class TestOBSFileContextMainOperations extends
    FileContextMainOperationsBaseTest {

    private OBSFileSystem fs;

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
        String fileSystem = conf.get(OBSTestConstants.TEST_FS_OBS_NAME);
        if (fileSystem == null || fileSystem.trim().length() == 0) {
            throw new Exception("Default file system not configured.");
        }

        URI uri = new URI(fileSystem);
        fs = OBSTestUtils.createTestFileSystem(conf);
        fc = FileContext.getFileContext(new DelegateToFileSystem(uri, fs,
            conf, fs.getScheme(), false) {
        }, conf);
        super.setUp();
    }

    @Override
    protected boolean listCorruptedBlocksSupported() {
        return false;
    }

    @Override
    @Test
    public void testSetVerifyChecksum() {
        skip("Unsupport.");
    }

    @Test
    @Override
    public void testCreateFlagCreateAppendExistingFile() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }else {
            super.testCreateFlagCreateAppendExistingFile();
        }
    }

    @Test
    @Override
    public void testWriteInNonExistentDirectory() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }else {
            super.testWriteInNonExistentDirectory();
        }
    }

    @Test
    @Override
    public void testCreateFlagAppendExistingFile() throws IOException {
        if(!fs.isFsBucket()) {
            return;
        }else {
            super.testCreateFlagAppendExistingFile();
        }
    }

    @AfterClass
    public static void clearBucket() throws IOException {
        OBSFSTestUtil.clearBucket();
    }
}

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

package org.apache.hadoop.fs.obs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractRootDirectoryTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.obs.OBSFSTestUtil;
import org.apache.hadoop.fs.obs.OBSFileSystem;
import org.apache.hadoop.fs.obs.OBSTestRule;
import org.apache.hadoop.fs.obs.OBSTestUtils;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

/**
 * Root directory test cases on obs file system.
 */
public class TestOBSContractRootDir extends AbstractContractRootDirectoryTest {

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    private OBSFileSystem fs;

    @Override
    public void setup() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        fs = OBSTestUtils.createTestFileSystem(conf);
        super.setup();
    }

    @Override
    protected AbstractFSContract createContract(final Configuration conf) {
        return new OBSContract(conf);
    }

    @Test
    @Override
    public void testRmNonEmptyRootDirNonRecursive() throws Throwable {
        if(!fs.isFsBucket()) {
            return;
        }else {
            super.testRmNonEmptyRootDirNonRecursive();
        }
    }

    @AfterClass
    public static void clearBucket() throws IOException {
        OBSFSTestUtil.clearBucket();
    }
}

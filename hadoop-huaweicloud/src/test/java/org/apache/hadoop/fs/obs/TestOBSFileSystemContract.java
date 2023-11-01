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
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;

import java.io.IOException;

/**
 * Tests a live OBS system. If your keys and bucket aren't specified, all tests
 * are marked as passed.
 * <p>
 * This uses BlockJUnit4ClassRunner because FileSystemContractBaseTest from
 * TestCase which uses the old Junit3 runner that doesn't ignore assumptions
 * properly making it impossible to skip the tests if we don't have a valid
 * bucket.
 **/
@Deprecated
public class TestOBSFileSystemContract extends FileSystemContractBaseTest {

    @Before
    public void setUp() throws Exception {
        skipTestCheck();
        Configuration conf = OBSContract.getConfiguration(null);
        conf.addResource(OBSContract.CONTRACT_XML);
        fs = OBSTestUtils.createTestFileSystem(conf);
        System.out.println("Begin to run testcase TestOBSFileSystemContract");
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.println("Finish run testcase TestOBSFileSystemContract success");
    }

    @Override
    public void testMkdirsWithUmask() {
        Assume.assumeTrue("unsupport.", true);
    }

    public void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @AfterClass
    public static void clearBucket() throws IOException {
        OBSFSTestUtil.clearBucket();
    }
}

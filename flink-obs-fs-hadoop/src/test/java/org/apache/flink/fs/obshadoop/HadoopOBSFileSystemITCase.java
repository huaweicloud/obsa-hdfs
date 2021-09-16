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

package org.apache.flink.fs.obshadoop;

import static org.junit.Assert.assertFalse;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.AbstractHadoopFileSystemITTest;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Unit tests for the S3 file system support via Hadoop's {@link
 * org.apache.hadoop.fs.obs.OBSFileSystem}.
 */
public class HadoopOBSFileSystemITCase extends AbstractHadoopFileSystemITTest {
    private static final String TEST_DATA_DIR = "HadoopOBSFileSystemITCase";

    @BeforeClass
    public static void setup() throws IOException {
        // check whether credentials exist
        // OBSTestCredentials.assumeCredentialsAvailable();

        // initialize configuration with valid credentials
        final Configuration conf = new Configuration();
        conf.setString("fs.obs.access.key", OBSTestCredentials.getOBSAccessKey());
        conf.setString("fs.obs.secret.key", OBSTestCredentials.getOBSSecretKey());
        conf.setString("fs.obs.endpoint", OBSTestCredentials.getOBSEndpoint());
        FileSystem.initialize(conf);

        basePath = new Path(OBSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
        // basePath = new Path("obs://obsa-bigdata-posix/" + TEST_DATA_DIR);
        fs = basePath.getFileSystem();
        consistencyToleranceNS = 0L; // 0 seconds

        // check for uniqueness of the test directory
        // directory must not yet exist
        assertFalse(fs.exists(basePath));
    }
}

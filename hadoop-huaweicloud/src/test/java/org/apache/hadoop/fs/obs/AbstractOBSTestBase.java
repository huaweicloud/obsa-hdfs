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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;

/**
 * An extension of the contract test base set up for OBS tests.
 */
@Deprecated
public abstract class AbstractOBSTestBase extends AbstractFSContractTestBase {

    protected static final Logger LOG =
        LoggerFactory.getLogger(AbstractOBSTestBase.class);

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
        return new OBSContract(conf);
    }

    @Override
    public void teardown() throws Exception {
        super.teardown();
        describe("closing file system");
        IOUtils.closeStream(getFileSystem());
    }

    @Before
    public void nameThread() {
        Thread.currentThread().setName("JUnit-" + methodName.getMethodName());
    }

    @Override
    protected int getTestTimeoutMillis() {
        return OBSTestConstants.OBS_TEST_TIMEOUT;
    }

    protected Configuration getConfiguration() {
        return getContract().getConf();
    }

    /**
     * Get the filesystem as an OBS filesystem.
     *
     * @return the typecast FS
     */
    @Override
    public OBSFileSystem getFileSystem() {
        return (OBSFileSystem) super.getFileSystem();
    }

    /**
     * Describe a test in the logs.
     *
     * @param text text to print
     * @param args arguments to format in the printing
     */
    protected void describe(String text, Object... args) {
        LOG.info("\n\n{}: {}\n",
            methodName.getMethodName(),
            String.format(text, args));
    }

    /**
     * Write a file, read it back, validate the dataset. Overwrites the file if
     * it is present
     *
     * @param name filename (will have the test path prepended to it)
     * @param len  length of file
     * @return the full path to the file
     * @throws IOException any IO problem
     */
    protected Path writeThenReadFile(String name, int len) throws IOException {
        Path path = path(name);
        byte[] data = dataset(len, 'a', 'z');
        writeDataset(getFileSystem(), path, data, data.length, 1024 * 1024,
            true);
        ContractTestUtils.verifyFileContents(getFileSystem(), path, data);
        return path;
    }

    /**
     * Assert that an exception failed with a specific status code.
     *
     * @param e    exception
     * @param code expected status code
     * @throws OBSIOException rethrown if the status code does not match.
     */
    protected void assertStatusCode(OBSIOException e, int code)
        throws OBSIOException {
        if (e.getCause().getResponseCode() != code) {
            throw e;
        }
    }
}

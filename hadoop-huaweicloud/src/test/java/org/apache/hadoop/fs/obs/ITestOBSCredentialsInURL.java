/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;

import static org.apache.hadoop.fs.obs.OBSTestConstants.TEST_FS_OBS_NAME;

/**
 * Tests that credentials can go into the URL. This includes a valid
 * set, and a check that an invalid set do at least get stripped out
 * of the final URI
 */
public class ITestOBSCredentialsInURL extends Assert {
    private OBSFileSystem fs;
    private Configuration conf;
    private static final Logger LOG =
        LoggerFactory.getLogger(ITestOBSCredentialsInURL.class);

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @Rule
    public Timeout testTimeout = new Timeout(30 * 60 * 1000);

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        conf = OBSContract.getConfiguration(null);
        fs = OBSTestUtils.createTestFileSystem(conf);
    }

    @After
    public void teardown() {
        IOUtils.closeStream(fs);
    }

    /**
     * Test instantiation.
     *
     * @throws Throwable
     */
    @Test
    // uri中携带credential信息，编码后ak、sk不包含/和+符号
    public void testInstantiateFromURL() throws Throwable {

        String accessKey = conf.get(OBSConstants.ACCESS_KEY);
        String secretKey = conf.get(OBSConstants.SECRET_KEY);
        String fsname = conf.getTrimmed(TEST_FS_OBS_NAME, "");
        Assume.assumeNotNull(fsname, accessKey, secretKey);
        URI original = new URI(fsname);
        URI secretsURI = createUriWithEmbeddedSecrets(original,
            accessKey, secretKey);
        if (secretKey.contains("/")) {
            assertTrue("test URI encodes the / symbol", secretsURI.toString().
                contains("%252F"));
        }
        if (secretKey.contains("+")) {
            assertTrue("test URI encodes the + symbol", secretsURI.toString().
                contains("%252B"));
        }
        assertFalse("Does not contain secrets", original.equals(secretsURI));

        conf.set(TEST_FS_OBS_NAME, secretsURI.toString());
        conf.unset(OBSConstants.ACCESS_KEY);
        conf.unset(OBSConstants.SECRET_KEY);
        String fsURI = fs.getUri().toString();
        assertFalse("FS URI contains a @ symbol", fsURI.contains("@"));
        assertFalse("FS URI contains a % symbol", fsURI.contains("%"));
        if (!original.toString().startsWith(fsURI)) {
            fail("Filesystem URI does not match original");
        }
        validate("original path", new Path(original));
        validate("bare path", new Path("/"));
        validate("secrets path", new Path(secretsURI));
        if (fs != null) {
            fs.close();
            fs = null;
        }
    }

    @Test
    // 测试标准化uri功能正常
    public void testCanonicalizeUri() throws Exception {
        assertNotNull("OBSFileSystem is null.", fs);
        URI testURI = new URI(OBSTestConstants.TEST_FS_OBS_NAME);
        URI canonicalizedURI = fs.canonicalizeUri(testURI);
        assertEquals(testURI, canonicalizedURI);
        if (fs != null) {
            fs.close();
            fs = null;
        }
    }

    @Test
    // 校验Path/URL符合OBSFS的规范
    public void testCheckPath() throws IOException {
        assertNotNull("OBSFileSystem is null.", fs);

        // 1. no schema
        Path testPath = new Path("/test/abc");
        fs.checkPath(testPath);

        // 2. schema not match
        testPath = new Path("sss://testfs/abc");
        boolean hasException = false;
        try {
            fs.checkPath(testPath);
        } catch (IllegalArgumentException e) {
            hasException = true;
        }
        assertTrue(hasException);

        // 3.1. host is null, fs be same with defaultFs
        conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
            OBSTestConstants.TEST_FS_OBS_NAME);
        testPath = new Path("obs:///test");
        hasException = false;
        try {
            fs.checkPath(testPath);
        } catch (IllegalArgumentException e) {
            hasException = true;
        }
        assertTrue(hasException);

        // 3.2. host is null, fs not same with defaultFs
        conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
            "obs://NOT-SAME-bucket-4521/");
        testPath = new Path("obs:///test");
        hasException = false;
        try {
            fs.checkPath(testPath);
        } catch (IllegalArgumentException e) {
            hasException = true;
        }
        assertTrue(hasException);
        if (fs != null) {
            fs.close();
            fs = null;
        }
    }

    private void validate(String text, Path path) throws IOException {
        try {
            fs.canonicalizeUri(path.toUri());
            fs.checkPath(path);
            assertTrue(text + " Not a directory",
                fs.getFileStatus(new Path("/")).isDirectory());
            fs.globStatus(path);
        } catch (Exception e) {
            LOG.debug("{} failure: {}", text, e, e);
            fail(text + " Test failed");
        }
    }

    private URI createUriWithEmbeddedSecrets(URI original,
        String accessKey,
        String secretKey) throws UnsupportedEncodingException {
        String encodedSecretKey = URLEncoder.encode(secretKey, "UTF-8");
        String formattedString = String.format("%s://%s:%s@%s/%s/",
            original.getScheme(),
            accessKey,
            encodedSecretKey,
            original.getHost(),
            original.getPath());
        URI testURI;
        try {
            testURI = new Path(formattedString).toUri();
        } catch (IllegalArgumentException e) {
            // inner cause is stripped to keep any secrets out of stack traces
            throw new IllegalArgumentException(
                "Could not encode Path: " + formattedString);
        }
        return testURI;
    }
}

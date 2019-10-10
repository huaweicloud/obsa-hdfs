/**
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

import com.obs.services.ObsClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.obs.OBSTestConstants.TEST_FS_OBS_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;

import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.junit.rules.TemporaryFolder;

import static org.apache.hadoop.fs.obs.Constants.*;
import static org.apache.hadoop.fs.obs.OBSUtils.*;
import static org.apache.hadoop.fs.obs.OBSTestUtils.*;

/**
 * OBS tests for configuration.
 */
public class ITestObsConfiguration {
  private static final String EXAMPLE_ID = "AKASOMEACCESSKEY";
  private static final String EXAMPLE_KEY =
          "RGV0cm9pdCBSZ/WQgY2xl/YW5lZCB1cAEXAMPLE";

  private Configuration conf;
  private OBSFileSystem fs;

  private static final Logger LOG =
          LoggerFactory.getLogger(ITestObsConfiguration.class);

  @Rule
  public Timeout testTimeout = new Timeout(
          OBSTestConstants.OBS_TEST_TIMEOUT
  );

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  /**
   * Test if custom endpoint is picked up.
   * <p>
   * The test expects {@link OBSTestConstants#CONFIGURATION_TEST_ENDPOINT}
   * to be defined in the Configuration
   * describing the endpoint of the bucket to which TEST_FS_OBS_NAME points。
   * Evidently, the bucket has to be hosted in the region denoted by the
   * endpoint for the test to succeed.
   * <p>
   *
   * @throws Exception
   */
  @Test
  public void testEndpoint() throws Exception {
    conf = new Configuration();
    String endpoint = conf.getTrimmed(
            OBSTestConstants.CONFIGURATION_TEST_ENDPOINT, "");
    if (endpoint.isEmpty()) {
      LOG.warn("Custom endpoint test skipped as " +
              OBSTestConstants.CONFIGURATION_TEST_ENDPOINT + "config " +
              "setting was not detected");
    } else {
      conf.set(Constants.ENDPOINT, endpoint);
      fs = OBSTestUtils.createTestFileSystem(conf);
      ObsClient obs = fs.getObsClient();
      String endPointRegion = "";
      // Differentiate handling of "obs-" and "obs." based endpoint identifiers
      String[] endpointParts = StringUtils.split(endpoint, '.');
      if (endpointParts.length == 3) {
        endPointRegion = endpointParts[0].substring(3);
      } else if (endpointParts.length == 4) {
        endPointRegion = endpointParts[1];
      } else {
        fail("Unexpected endpoint");
      }
      assertEquals("Endpoint config setting and bucket location differ: ",
              endPointRegion, obs.getBucketLocation(fs.getUri().getHost()));
    }
  }

  @Test
  public void testProxyConnection() throws Exception {
    conf = new Configuration();
    conf.setInt(Constants.MAX_ERROR_RETRIES, 2);
    conf.set(Constants.PROXY_HOST, "127.0.0.1");
    conf.setInt(Constants.PROXY_PORT, 1);
    String proxy =
            conf.get(Constants.PROXY_HOST) + ":" + conf.get(Constants.PROXY_PORT);
    try {
      fs = OBSTestUtils.createTestFileSystem(conf);
      fail("Expected a connection error for proxy server at " + proxy);
    } catch (OBSIOException e) {
      // expected
    }
  }

  @Test
  public void testProxyPortWithoutHost() throws Exception {
    conf = new Configuration();
    conf.setInt(Constants.MAX_ERROR_RETRIES, 2);
    conf.setInt(Constants.PROXY_PORT, 1);
    try {
      fs = OBSTestUtils.createTestFileSystem(conf);
      fail("Expected a proxy configuration error");
    } catch (OBSIOException e) {
      String msg = e.toString();
      if (!msg.contains(Constants.PROXY_HOST) &&
              !msg.contains(Constants.PROXY_PORT)) {
        //expected
      }
    }
  }


  void provisionAccessKeys(final Configuration conf) throws Exception {
    // add our creds to the provider
    final CredentialProvider provider =
            CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry(Constants.ACCESS_KEY,
            EXAMPLE_ID.toCharArray());
    provider.createCredentialEntry(Constants.SECRET_KEY,
            EXAMPLE_KEY.toCharArray());
    provider.flush();
  }

  @Test
  public void testCredsFromUserInfo() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
            file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
            jks.toString());

    provisionAccessKeys(conf);

    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    URI uriWithUserInfo = new URI("obs://123:456@foobar");
    OBSLoginHelper.Login creds =
            OBSUtils.getOBSAccessKeys(uriWithUserInfo, conf);
    assertEquals("AccessKey incorrect.", "123", creds.getUser());
    assertEquals("SecretKey incorrect.", "456", creds.getPassword());
  }

  @Test
  public void testExcludingS3ACredentialProvider() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
            file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
            "jceks://obs/foobar," + jks.toString());

    // first make sure that the obs based provider is removed
    Configuration c = ProviderUtils.excludeIncompatibleCredentialProviders(
            conf, OBSFileSystem.class);
    String newPath = conf.get(
            CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH);
    assertFalse("Provider Path incorrect", newPath.contains("obs://"));

    // now let's make sure the new path is created by the OBSFileSystem
    // and the integration still works. Let's provision the keys through
    // the altered configuration instance and then try and access them
    // using the original config with the obs provider in the path.
    provisionAccessKeys(c);

    conf.set(Constants.ACCESS_KEY, EXAMPLE_ID + "LJM");
    URI uriWithUserInfo = new URI("obs://123:456@foobar");
    OBSLoginHelper.Login creds =
            OBSUtils.getOBSAccessKeys(uriWithUserInfo, conf);
    assertEquals("AccessKey incorrect.", "123", creds.getUser());
    assertEquals("SecretKey incorrect.", "456", creds.getPassword());

  }

  @Test
  public void testCloseIdempotent() throws Throwable {
    conf = new Configuration();
    fs = OBSTestUtils.createTestFileSystem(conf);
    fs.close();
    fs.close();
  }

  @Test
  public void testDirectoryAllocatorDefval() throws Throwable {
    conf = new Configuration();
    conf.unset(Constants.BUFFER_DIR);
    fs = OBSTestUtils.createTestFileSystem(conf);
    File tmp = fs.createTmpFileForWrite("out-", 1024, conf);
    assertTrue("not found: " + tmp, tmp.exists());
    tmp.delete();
  }

  @Test
  public void testDirectoryAllocatorRR() throws Throwable {
    File dir1 = GenericTestUtils.getRandomizedTestDir();
    File dir2 = GenericTestUtils.getRandomizedTestDir();
    dir1.mkdirs();
    dir2.mkdirs();
    conf = new Configuration();
    conf.set(Constants.BUFFER_DIR, dir1 + ", " + dir2);
    fs = OBSTestUtils.createTestFileSystem(conf);
    File tmp1 = fs.createTmpFileForWrite("out-", 1024, conf);
    tmp1.delete();
    File tmp2 = fs.createTmpFileForWrite("out-", 1024, conf);
    tmp2.delete();
    assertNotEquals("round robin not working",
            tmp1.getParent(), tmp2.getParent());
  }

  @Test
  public void testReadAheadRange() throws Exception {
    conf = new Configuration();
    conf.set(Constants.READAHEAD_RANGE, "300K");
    fs = OBSTestUtils.createTestFileSystem(conf);
    assertNotNull(fs);
    long readAheadRange = fs.getReadAheadRange();
    assertNotNull(readAheadRange);
    assertEquals("Read Ahead Range Incorrect.", 300 * 1024, readAheadRange);
  }

  @Test
  public void testUsernameFromUGI() throws Throwable {
    final String alice = "alice";
    UserGroupInformation fakeUser =
            UserGroupInformation.createUserForTesting(alice,
                    new String[]{"users", "administrators"});
    conf = new Configuration();
    fs = fakeUser.doAs(new PrivilegedExceptionAction<OBSFileSystem>() {
      @Override
      public OBSFileSystem run() throws Exception{
        return OBSTestUtils.createTestFileSystem(conf);
      }
    });
    assertEquals("username", alice, fs.getUsername());
    OBSFileStatus status = fs.getFileStatus(new Path("/"));
    assertEquals("owner in " + status, alice, status.getOwner());
    assertEquals("group in " + status, alice, status.getGroup());
  }

  /**
   * Reads and returns a field from an object using reflection.  If the field
   * cannot be found, is null, or is not the expected type, then this method
   * fails the test.
   *
   * @param target object to read
   * @param fieldType type of field to read, which will also be the return type
   * @param fieldName name of field to read
   * @return field that was read
   * @throws IllegalAccessException if access not allowed
   */
  private static <T> T getField(Object target, Class<T> fieldType,
                                String fieldName) throws IllegalAccessException {
    Object obj = FieldUtils.readField(target, fieldName, true);
    assertNotNull(String.format(
            "Could not read field named %s in object with class %s.", fieldName,
            target.getClass().getName()), obj);
    assertTrue(String.format(
            "Unexpected type found for field named %s, expected %s, actual %s.",
            fieldName, fieldType.getName(), obj.getClass().getName()),
            fieldType.isAssignableFrom(obj.getClass()));
    return fieldType.cast(obj);
  }

  @Test
  public void testBucketConfigurationPropagation() throws Throwable {
    Configuration config = new Configuration(false);
    setBucketOption(config, "b", "base", "1024");
    String basekey = "fs.obs.base";
    assertOptionEquals(config, basekey, null);
    String bucketKey = "fs.obs.bucket.b.base";
    assertOptionEquals(config, bucketKey, "1024");
    Configuration updated = propagateBucketOptions(config, "b");
    assertOptionEquals(updated, basekey, "1024");
    // original conf is not updated
    assertOptionEquals(config, basekey, null);

    String[] sources = updated.getPropertySources(basekey);
    assertEquals(1, sources.length);
    String sourceInfo = sources[0];
    assertTrue("Wrong source " + sourceInfo, sourceInfo.contains(bucketKey));
  }

  @Test
  public void testBucketConfigurationPropagationResolution() throws Throwable {
    Configuration config = new Configuration(false);
    String basekey = "fs.obs.base";
    String baseref = "fs.obs.baseref";
    String baseref2 = "fs.obs.baseref2";
    config.set(basekey, "orig");
    config.set(baseref2, "${fs.obs.base}");
    setBucketOption(config, "b", basekey, "1024");
    setBucketOption(config, "b", baseref, "${fs.obs.base}");
    Configuration updated = propagateBucketOptions(config, "b");
    assertOptionEquals(updated, basekey, "1024");
    assertOptionEquals(updated, baseref, "1024");
    assertOptionEquals(updated, baseref2, "1024");
  }

  @Test
  public void testMultipleBucketConfigurations() throws Throwable {
    Configuration config = new Configuration(false);
    setBucketOption(config, "b", USER_AGENT_PREFIX, "UA-b");
    setBucketOption(config, "c", USER_AGENT_PREFIX, "UA-c");
    config.set(USER_AGENT_PREFIX, "UA-orig");
    Configuration updated = propagateBucketOptions(config, "c");
    assertOptionEquals(updated, USER_AGENT_PREFIX, "UA-c");
  }

  @Test
  public void testBucketConfigurationSkipsUnmodifiable() throws Throwable {
    Configuration config = new Configuration(false);
    String impl = "fs.obs.impl";
    config.set(impl, "orig");
    setBucketOption(config, "b", impl, "b");
    String metastoreImpl = "fs.obs.metadatastore.impl";
    String ddb = "org.apache.hadoop.fs.obs.s3guard.DynamoDBMetadataStore";
    setBucketOption(config, "b", metastoreImpl, ddb);
    setBucketOption(config, "b", "impl2", "b2");
    setBucketOption(config, "b", "bucket.b.loop", "b3");
    assertOptionEquals(config, "fs.obs.bucket.b.impl", "b");

    Configuration updated = propagateBucketOptions(config, "b");
    assertOptionEquals(updated, impl, "orig");
    assertOptionEquals(updated, "fs.obs.impl2", "b2");
    assertOptionEquals(updated, metastoreImpl, ddb);
    assertOptionEquals(updated, "fs.obs.bucket.b.loop", null);
  }

  @Test
  public void testConfOptionPropagationToFS() throws Exception {
    Configuration config = new Configuration();
    String testFSName = config.getTrimmed(TEST_FS_OBS_NAME, "");
    String bucket = new URI(testFSName).getHost();
    setBucketOption(config, bucket, "propagation", "propagated");
    fs = OBSTestUtils.createTestFileSystem(config);
    Configuration updated = fs.getConf();
    assertOptionEquals(updated, "fs.obs.propagation", "propagated");
  }

  @Test
  public void testSecurityCredentialPropagationNoOverride() throws Exception {
    Configuration config = new Configuration();
    config.set(CREDENTIAL_PROVIDER_PATH, "base");
    patchSecurityCredentialProviders(config);
    assertOptionEquals(config, CREDENTIAL_PROVIDER_PATH,
            "base");
  }

  @Test
  public void testSecurityCredentialPropagationOverrideNoBase()
          throws Exception {
    Configuration config = new Configuration();
    config.unset(CREDENTIAL_PROVIDER_PATH);
    config.set(OBS_SECURITY_CREDENTIAL_PROVIDER_PATH, "override");
    patchSecurityCredentialProviders(config);
    assertOptionEquals(config, CREDENTIAL_PROVIDER_PATH,
            "override");
  }

  @Test
  public void testSecurityCredentialPropagationOverride() throws Exception {
    Configuration config = new Configuration();
    config.set(CREDENTIAL_PROVIDER_PATH, "base");
    config.set(OBS_SECURITY_CREDENTIAL_PROVIDER_PATH, "override");
    patchSecurityCredentialProviders(config);
    assertOptionEquals(config, CREDENTIAL_PROVIDER_PATH,
            "override,base");
    Collection<String> all = config.getStringCollection(
            CREDENTIAL_PROVIDER_PATH);
    assertTrue(all.contains("override"));
    assertTrue(all.contains("base"));
  }

  @Test
  public void testSecurityCredentialPropagationEndToEnd() throws Exception {
    Configuration config = new Configuration();
    config.set(CREDENTIAL_PROVIDER_PATH, "base");
    setBucketOption(config, "b", OBS_SECURITY_CREDENTIAL_PROVIDER_PATH,
            "override");
    Configuration updated = propagateBucketOptions(config, "b");

    patchSecurityCredentialProviders(updated);
    assertOptionEquals(updated, CREDENTIAL_PROVIDER_PATH,
            "override,base");
  }

}

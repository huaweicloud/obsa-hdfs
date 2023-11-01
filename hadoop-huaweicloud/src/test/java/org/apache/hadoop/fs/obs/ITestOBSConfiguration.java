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

import com.obs.services.ObsClient;
import com.obs.services.internal.ObsProperties;
import com.obs.services.internal.RestConnectionService;
import com.obs.services.internal.RestStorageService;

import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.fs.obs.OBSConstants.OBS_SECURITY_CREDENTIAL_PROVIDER_PATH;
import static org.apache.hadoop.fs.obs.OBSConstants.USER_AGENT_PREFIX;
import static org.apache.hadoop.fs.obs.OBSTestConstants.TEST_FS_OBS_NAME;
import static org.apache.hadoop.fs.obs.OBSTestUtils.assertOptionEquals;
import static org.apache.hadoop.fs.obs.OBSTestUtils.setBucketOption;
import static org.apache.hadoop.fs.obs.OBSCommonUtils.CREDENTIAL_PROVIDER_PATH;
import static org.apache.hadoop.fs.obs.OBSCommonUtils.patchSecurityCredentialProviders;
import static org.apache.hadoop.fs.obs.OBSCommonUtils.propagateBucketOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * OBS tests for configuration.
 */
public class ITestOBSConfiguration {
    private static final String EXAMPLE_ID = "AKASOMEACCESSKEY";

    private static final String EXAMPLE_KEY =
        "RGV0cm9pdCBSZ/WQgY2xl/YW5lZCB1cAEXAMPLE";

    private Configuration conf;

    private OBSFileSystem fs;

    private static final Logger LOG =
        LoggerFactory.getLogger(ITestOBSConfiguration.class);

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @Rule
    public Timeout testTimeout = new Timeout(
        OBSTestConstants.OBS_TEST_TIMEOUT
    );

    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        conf = OBSContract.getConfiguration(null);
        fs = OBSTestUtils.createTestFileSystem(conf);;
    }

    @Test
    // 使用无效的proxy host和port，创建fs会超时失败
    public void testProxyConnection() throws Exception {
        conf.setInt(OBSConstants.MAX_ERROR_RETRIES, 2);
        conf.set(OBSConstants.PROXY_HOST, "127.0.0.1");
        conf.setInt(OBSConstants.PROXY_PORT, 1);
        String proxy =
            conf.get(OBSConstants.PROXY_HOST) + ":" + conf.get(
                OBSConstants.PROXY_PORT);
        try {
            fs = OBSTestUtils.createTestFileSystem(conf);
            fail("Expected a connection error for proxy server at " + proxy);
        } catch (OBSIOException e) {
            // expected
        }
    }

    @Test
    // proxy未指定host时，创建fs会超时失败
    public void testProxyPortWithoutHost() throws Exception {
        conf.setInt(OBSConstants.MAX_ERROR_RETRIES, 2);
        conf.setInt(OBSConstants.PROXY_PORT, 1);
        try {
            fs = OBSTestUtils.createTestFileSystem(conf);
            fail("Expected a proxy configuration error");
        } catch (OBSIOException e) {
            String msg = e.toString();
            if (!msg.contains(OBSConstants.PROXY_HOST) &&
                !msg.contains(OBSConstants.PROXY_PORT)) {
                //expected
            }
        }
    }

    void provisionAccessKeys(final Configuration conf) throws Exception {
        // add our creds to the provider
        final CredentialProvider provider =
            CredentialProviderFactory.getProviders(conf).get(0);
        provider.createCredentialEntry(OBSConstants.ACCESS_KEY,
            EXAMPLE_ID.toCharArray());
        provider.createCredentialEntry(OBSConstants.SECRET_KEY,
            EXAMPLE_KEY.toCharArray());
        provider.flush();
    }

    // 从userInfo中解析credentials，能正确解析出ak、sk
    public void testCredsFromUserInfo() throws Exception {
        final File file = tempDir.newFile("test.jks");
        final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
            file.toURI());
        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
            jks.toString());

        LOG.warn("file URI: {}, jks: {}", file.toURI(), jks.toString());
        System.out.println("file URI: " + file.toURI() + ", jks: " + "jceks://obs/foobar," + jks.toString());

        LOG.warn("provider: {}", conf.get(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH));
        System.out.println("provider: " + conf.get(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH));

        try {
            provisionAccessKeys(conf);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            System.exit(-1);
        }

        conf.set(OBSConstants.ACCESS_KEY, EXAMPLE_ID + "LJM");
        URI uriWithUserInfo = new URI("obs://123:456@foobar");
        OBSLoginHelper.Login creds =
            OBSCommonUtils.getOBSAccessKeys(uriWithUserInfo, conf);
        assertEquals("AccessKey incorrect.", "123", creds.getUser());
        assertEquals("SecretKey incorrect.", "456", creds.getPassword());
    }

    // 测试可正常排除不兼容的S3ACredentialProvider
    public void testExcludingS3ACredentialProvider() throws Exception {
        // set up conf to have a cred provider
        final File file = tempDir.newFile("test.jks");
        final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
            file.toURI());
        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
            "jceks://obs/foobar," + jks.toString());

        LOG.warn("file URI: {}, jks: {}", file.toURI(), jks.toString());
        System.out.println("file URI: " + file.toURI() + ", jks: " + "jceks://obs/foobar," + jks.toString());

        LOG.warn("provider: {}", conf.get(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH));
        System.out.println("provider: " + conf.get(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH));

        // first make sure that the obs based provider is removed
        Configuration c = ProviderUtils.excludeIncompatibleCredentialProviders(
            conf, OBSFileSystem.class);
        String newPath = conf.get(
            CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH);
        assertFalse("Provider Path incorrect", newPath.contains("obs://"));

        LOG.warn("after exclude, newPath: {}", newPath);
        System.out.println("after exclude, newPath: " + newPath);

        // now let's make sure the new path is created by the OBSFileSystem
        // and the integration still works. Let's provision the keys through
        // the altered configuration instance and then try and access them
        // using the original config with the obs provider in the path.
        try {
            provisionAccessKeys(c);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        conf.set(OBSConstants.ACCESS_KEY, EXAMPLE_ID + "LJM");
        URI uriWithUserInfo = new URI("obs://123:456@foobar");
        OBSLoginHelper.Login creds =
            OBSCommonUtils.getOBSAccessKeys(uriWithUserInfo, conf);
        assertEquals("AccessKey incorrect.", "123", creds.getUser());
        assertEquals("SecretKey incorrect.", "456", creds.getPassword());

    }

    // @Test
    // public void testDirectoryAllocatorRR() throws Throwable {
    //     String localDir1 = "local_dir1";
    //     OBSFSTestUtil.createLocalTestDir(localDir1);
    //     String localDir2 = "local_dir2";
    //     OBSFSTestUtil.createLocalTestDir(localDir2);
    //     File dir1 = new File(localDir1);
    //     File dir2 = new File(localDir2);
    //     dir1.mkdirs();
    //     dir2.mkdirs();
    //     conf.set(OBSConstants.BUFFER_DIR, dir1 + ", " + dir2);
    //     fs = OBSTestUtils.createTestFileSystem(conf);
    //     File tmp1 = createTmpFileForWrite("out-", 1024, conf);
    //     tmp1.delete();
    //     File tmp2 = createTmpFileForWrite("out-", 1024, conf);
    //     tmp2.delete();
    //     assertNotEquals("round robin not working",
    //         tmp1.getParent(), tmp2.getParent());
    //
    //     File tmp = createTmpFileForWrite("out-", 1024, conf);
    //     assertTrue("not found: " + tmp, tmp.exists());
    //     tmp.delete();
    // }

    @Test
    // 测试readahead.range可正常配置
    public void testReadAheadRange() throws Exception {
        conf.set(OBSConstants.READAHEAD_RANGE, "300K");
        fs = OBSTestUtils.createTestFileSystem(conf);
        assertNotNull(fs);
        long readAheadRange = fs.getReadAheadRange();
        assertNotNull(readAheadRange);
        assertEquals("Read Ahead Range Incorrect.", 300 * 1024, readAheadRange);
    }

    @Test
    // 测试可正常获取UGI信息
    public void testUsernameFromUGI() throws Throwable {
        final String alice = "alice";
        UserGroupInformation fakeUser =
            UserGroupInformation.createUserForTesting(alice,
                new String[] {"users", "administrators"});
        fs = fakeUser.doAs(new PrivilegedExceptionAction<OBSFileSystem>() {
            @Override
            public OBSFileSystem run() throws Exception {
                return OBSTestUtils.createTestFileSystem(conf);
            }
        });
        assertEquals("username", alice, fs.getShortUserName());
        FileStatus status = fs.getFileStatus(new Path("/"));
        assertEquals("owner in " + status, alice, status.getOwner());
        assertEquals("group in " + status, alice, status.getGroup());
    }

    /**
     * Reads and returns a field from an object using reflection.  If the field
     * cannot be found, is null, or is not the expected type, then this method
     * fails the test.
     *
     * @param target    object to read
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
    // 测试可正常将桶特定配置转化成通用obs配置
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
        assertTrue("Wrong source " + sourceInfo,
            sourceInfo.contains(bucketKey));
    }

    @Test
    // 测试可正常解析配置文件中定义的变量字段
    public void testBucketConfigurationPropagationResolution()
        throws Throwable {
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
    // key重复的桶配置项，解析到的值为最后一个配置项的值
    public void testMultipleBucketConfigurations() throws Throwable {
        Configuration config = new Configuration(false);
        setBucketOption(config, "b", USER_AGENT_PREFIX, "UA-b");
        setBucketOption(config, "c", USER_AGENT_PREFIX, "UA-c");
        config.set(USER_AGENT_PREFIX, "UA-orig");
        Configuration updated = propagateBucketOptions(config, "c");
        assertOptionEquals(updated, USER_AGENT_PREFIX, "UA-c");
    }

    @Test
    // 传递桶特定配置到OBS通用配置时，不会覆盖OBS通用配置中相同配置项
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
    // 桶特定配置传递到OBS通用配置中，fs能正确识别到
    public void testConfOptionPropagationToFS() throws Exception {
        String testFSName = conf.getTrimmed(TEST_FS_OBS_NAME, "");
        String bucket = new URI(testFSName).getHost();
        setBucketOption(conf, bucket, "propagation", "propagated");
        fs = OBSTestUtils.createTestFileSystem(conf);
        Configuration updated = fs.getConf();
        assertOptionEquals(updated, "fs.obs.propagation", "propagated");
    }

    @Test
    // 添加的obs CredentialProviders不会覆盖hadoop CredentialProviders
    public void testSecurityCredentialPropagationNoOverride() {
        conf.set(CREDENTIAL_PROVIDER_PATH, "base");
        patchSecurityCredentialProviders(conf);
        assertOptionEquals(conf, CREDENTIAL_PROVIDER_PATH,
            "base");
    }

    @Test
    // 未设置hadoop CredentialProviders时，添加的obs CredentialProviders为全集
    public void testSecurityCredentialPropagationOverrideNoBase() {
        conf.unset(CREDENTIAL_PROVIDER_PATH);
        conf.set(OBS_SECURITY_CREDENTIAL_PROVIDER_PATH, "override");
        patchSecurityCredentialProviders(conf);
        assertOptionEquals(conf, CREDENTIAL_PROVIDER_PATH,
            "override");
    }

    @Test
    // 同时设置hadoop CredentialProviders和obs CredentialProviders时，全集为两者集合
    public void testSecurityCredentialPropagationOverride() {
        conf.set(CREDENTIAL_PROVIDER_PATH, "base");
        conf.set(OBS_SECURITY_CREDENTIAL_PROVIDER_PATH, "override");
        patchSecurityCredentialProviders(conf);
        assertOptionEquals(conf, CREDENTIAL_PROVIDER_PATH,
            "override,base");
        Collection<String> all = conf.getStringCollection(
            CREDENTIAL_PROVIDER_PATH);
        assertTrue(all.contains("override"));
        assertTrue(all.contains("base"));
    }

    @Test
    // 可将桶特定CredentialProviders配置传递到OBS通用CredentialProviders配置中
    public void testSecurityCredentialPropagationEndToEnd() {
        conf.set(CREDENTIAL_PROVIDER_PATH, "base");
        setBucketOption(conf, "b", OBS_SECURITY_CREDENTIAL_PROVIDER_PATH,
            "override");
        Configuration updated = propagateBucketOptions(conf, "b");
        patchSecurityCredentialProviders(updated);
        assertOptionEquals(updated, CREDENTIAL_PROVIDER_PATH,
            "override,base");
    }

    @Test
    // 测试可正确配置鉴权协商开关
    public void testSetAuthTypeNegotiation()
        throws Exception, NoSuchFieldException, SecurityException {
        Configuration conf = OBSContract.getConfiguration(null);

        Pattern pattern = Pattern.compile(
            "^((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)$");
        Matcher m = pattern.matcher(conf.get(OBSConstants.ENDPOINT));

        // 1、set to true, when endpoint is IP Address, authTypeNegotiationEnabled should be false;
        // otherwise should be true
        conf.set(OBSConstants.SDK_AUTH_TYPE_NEGOTIATION_ENABLE, "true");
        OBSFileSystem obsFs = OBSTestUtils.createTestFileSystem(conf);
        RestStorageService client = obsFs.getObsClient();
        Field propertiesField = RestConnectionService.class.getDeclaredField(
            "obsProperties");

        propertiesField.setAccessible(true);
        ObsProperties properties = (ObsProperties) propertiesField.get(client);
        String authTypeNegotiationEnabled = properties
            .getStringProperty("httpclient.auth-type-negotiation", "false");
        if (m.matches()) {
            assertTrue(authTypeNegotiationEnabled.equalsIgnoreCase("false"));
        } else {
            assertTrue(authTypeNegotiationEnabled.equalsIgnoreCase("true"));
        }
        obsFs.close();

        // 2、set to false, authTypeNegotiationEnabled should be false
        conf.set(OBSConstants.SDK_AUTH_TYPE_NEGOTIATION_ENABLE, "false");
        obsFs = OBSTestUtils.createTestFileSystem(conf);
        client = (RestStorageService) obsFs.getObsClient();
        properties = (ObsProperties) propertiesField.get(client);
        authTypeNegotiationEnabled = properties
            .getStringProperty("httpclient.auth-type-negotiation", "false");
        assertTrue(authTypeNegotiationEnabled.equalsIgnoreCase("false"));
        obsFs.close();

        // 3、set to other value, authTypeNegotiationEnabled should be default value false
        conf.set(OBSConstants.SDK_AUTH_TYPE_NEGOTIATION_ENABLE, "invalid");
        obsFs = OBSTestUtils.createTestFileSystem(conf);
        client = (RestStorageService) obsFs.getObsClient();
        properties = (ObsProperties) propertiesField.get(client);
        authTypeNegotiationEnabled = properties
            .getStringProperty("httpclient.auth-type-negotiation", "false");
        assertTrue(authTypeNegotiationEnabled.equalsIgnoreCase("false"));
        obsFs.close();

        // 4、unset, authTypeNegotiationEnabled should be false
        conf.unset(OBSConstants.SDK_AUTH_TYPE_NEGOTIATION_ENABLE);
        obsFs = OBSTestUtils.createTestFileSystem(conf);
        client = (RestStorageService) obsFs.getObsClient();
        properties = (ObsProperties) propertiesField.get(client);
        authTypeNegotiationEnabled = properties
            .getStringProperty("httpclient.auth-type-negotiation", "false");
        assertTrue(authTypeNegotiationEnabled.equalsIgnoreCase("false"));
        obsFs.close();
    }

    // @Test
    // // 测试有无CanonicalServiceName配置项时，getCanonicalServiceName能返回正确的值
    // public void testGetCanonicalServiceName() throws Exception {
    //     // 1、未配置CanonicalServiceName开关时，接口默认返回null
    //     Configuration conf = OBSContract.getConfiguration(null);
    //     OBSFileSystem obsFs = OBSTestUtils.createTestFileSystem(conf);
    //     String canonicalServiceName = obsFs.getCanonicalServiceName();
    //     assertEquals("expected null, but get " + canonicalServiceName,
    //         null, canonicalServiceName);
    //     obsFs.close();
    //
    //     // 2、配置CanonicalServiceName开关为开启时，接口返回字符串obs://{bucketName}
    //     conf.setBoolean(OBSConstants.GET_CANONICAL_SERVICE_NAME_ENABLE, true);
    //     obsFs = OBSTestUtils.createTestFileSystem(conf);
    //     canonicalServiceName = obsFs.getCanonicalServiceName();
    //     String expected = obsFs.getScheme() + "://" + obsFs.getBucket();
    //     assertEquals("expected " + expected + " but get " + canonicalServiceName,
    //         expected,  canonicalServiceName);
    //     obsFs.close();
    //
    //     // 3、配置CanonicalServiceName开关为关闭时，接口返回null
    //     conf.setBoolean(OBSConstants.GET_CANONICAL_SERVICE_NAME_ENABLE, false);
    //     obsFs = OBSTestUtils.createTestFileSystem(conf);
    //     canonicalServiceName = obsFs.getCanonicalServiceName();
    //     assertEquals("expected null, but get " + canonicalServiceName,
    //         null, canonicalServiceName);
    //     obsFs.close();
    // }
}

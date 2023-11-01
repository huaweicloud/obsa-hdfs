/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.hadoop.fs.obs.security;

import static org.apache.hadoop.fs.obs.security.MockAuthorizeProvider.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.obs.OBSFileSystem;
import org.apache.hadoop.fs.obs.OBSTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * description
 *
 * @since 2021-10-13
 */
public class AuthorizeProviderTest {
    private static OBSFileSystem fs;
    private static FileSystem fs_with_auth;

    private static Path TEST_BASE_FOLDER_PATH;

    private static Path TEST_READ_ONLY_FILE_PATH_0;
    private static Path TEST_READ_ONLY_FOLDER_PATH;

    private static Path TEST_WRITE_ONLY_FILE_PATH_0;
    private static Path TEST_WRITE_ONLY_FILE_PATH_1;
    private static Path TEST_WRITE_ONLY_FOLDER_PATH;

    private static Path TEST_READ_WRITE_FILE_PATH;
    private static Path TEST_READ_WRITE_FOLDER_PATH;

    @Before
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        fs = OBSTestUtils.createTestFileSystem(conf);

        conf.set("fs.obs.authorize.provider","org.apache.hadoop.fs.obs.security.MockAuthorizeProvider");
        fs_with_auth = OBSTestUtils.createTestFileSystem(conf);

        TEST_BASE_FOLDER_PATH = new Path(fs_with_auth.getUri() + "/" + TEST_BASE_FOLDER);
        TEST_READ_ONLY_FILE_PATH_0 = new Path(fs_with_auth.getUri() + "/" + TEST_READ_ONLY_FILE_0);
        TEST_READ_ONLY_FOLDER_PATH = new Path(fs_with_auth.getUri() + "/" + TEST_READ_ONLY_FOLDER);

        TEST_WRITE_ONLY_FILE_PATH_0 = new Path(fs_with_auth.getUri() + "/" + TEST_WRITE_ONLY_FILE_0);
        TEST_WRITE_ONLY_FILE_PATH_1 = new Path(fs_with_auth.getUri() + "/" + TEST_WRITE_ONLY_FILE_1);
        TEST_WRITE_ONLY_FOLDER_PATH = new Path(fs_with_auth.getUri() + "/" + TEST_WRITE_ONLY_FOLDER);

        TEST_READ_WRITE_FILE_PATH = new Path(fs_with_auth.getUri() + "/" + TEST_READ_WRITE_FILE);
        TEST_READ_WRITE_FOLDER_PATH = new Path(fs_with_auth.getUri() + "/" + TEST_READ_WRITE_FOLDER);
    }

    @After
    public void tearDown() throws Exception {
        fs.delete(TEST_BASE_FOLDER_PATH,true);
        fs.close();
        fs_with_auth.close();
    }

    @Test
    public void testCreateFileAuthorized() throws Exception {
        fs_with_auth.create(TEST_WRITE_ONLY_FILE_PATH_0);
    }

    @Test
    public void testCreateFileUnauthorized() {
        try {
            fs_with_auth.create(TEST_READ_ONLY_FILE_PATH_0);
        } catch (IOException e) {
            Assert.assertTrue("testCreateFileUnauthorized",e instanceof OBSAuthorizationException);
        }
    }

    @Test
    public void testAppendFileAuthorized() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
        fs_with_auth.append(TEST_WRITE_ONLY_FILE_PATH_0).close();
    }

    @Test
    public void testAppendFileUnauthorized() {
        if (!fs.isFsBucket()) {
            return;
        }
        try {
            fs.create(TEST_READ_ONLY_FILE_PATH_0).close();
            fs_with_auth.append(TEST_READ_ONLY_FILE_PATH_0).close();
        } catch (IOException e) {
            Assert.assertTrue("testAppendFileUnauthorized",e instanceof OBSAuthorizationException);
        }
    }

    @Test
    public void testTruncateFileAuthorized() throws Exception {
        if (!fs.isFsBucket()) {
            return;
        }
        fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
        fs_with_auth.truncate(TEST_WRITE_ONLY_FILE_PATH_0,0);
    }

    @Test
    public void testTruncateFileUnauthorized() {
        if (!fs.isFsBucket()) {
            return;
        }
        try {
            fs.create(TEST_READ_ONLY_FILE_PATH_0).close();
            fs_with_auth.truncate(TEST_READ_ONLY_FILE_PATH_0,0);
        } catch (IOException e) {
            Assert.assertTrue("testTruncateFileUnauthorized",e instanceof OBSAuthorizationException);
        }
    }

    @Test
    public void testMkdirAuthorized() throws Exception {
        fs_with_auth.mkdirs(TEST_WRITE_ONLY_FOLDER_PATH);
    }

    @Test
    public void testMkdirUnauthorized() {
        try {
            fs_with_auth.mkdirs(TEST_READ_ONLY_FOLDER_PATH);
        } catch (IOException e) {
            Assert.assertTrue("testMkdirUnauthorized",e instanceof OBSAuthorizationException);
        }
    }

    @Test
    public void testOpenAuthorized() throws Exception {
        fs.create(TEST_READ_ONLY_FILE_PATH_0).close();
        fs_with_auth.open(TEST_READ_ONLY_FILE_PATH_0).close();
    }

    @Test
    public void testOpenUnauthorized() {
        try {
            fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
            fs_with_auth.open(TEST_WRITE_ONLY_FILE_PATH_0).close();
        } catch (IOException e) {
            Assert.assertTrue("testMkdirUnauthorized",e instanceof OBSAuthorizationException);
        }
    }

    @Test
    public void testDeleteAuthorized() throws Exception {
        fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
        fs_with_auth.delete(TEST_WRITE_ONLY_FILE_PATH_0,false);

        fs.mkdirs(TEST_WRITE_ONLY_FOLDER_PATH);
        fs_with_auth.delete(TEST_WRITE_ONLY_FOLDER_PATH,true);
    }

    @Test
    public void testDeleteUnauthorized() {
        try {
            fs.create(TEST_READ_ONLY_FILE_PATH_0).close();
            fs_with_auth.delete(TEST_READ_ONLY_FILE_PATH_0,false);
        } catch (IOException e) {
            Assert.assertTrue("testDeleteUnauthorized",e instanceof OBSAuthorizationException);
        }

        try {
            fs.mkdirs(TEST_READ_ONLY_FOLDER_PATH);
            fs_with_auth.delete(TEST_READ_ONLY_FOLDER_PATH,true);
        } catch (IOException e) {
            Assert.assertTrue("testDeleteUnauthorized",e instanceof OBSAuthorizationException);
        }
    }

    @Test
    public void testListRoot() throws Exception {
        try {
            Path root = new Path(fs_with_auth.getUri() + "/");
            fs_with_auth.listStatus(root);
        } catch (IOException e) {
            Assert.assertTrue("testListRoot",e instanceof OBSAuthorizationException);
        }
    }

    @Test
    public void testListAuthorized() throws Exception {
        fs.create(TEST_READ_ONLY_FILE_PATH_0).close();
        fs_with_auth.listStatus(TEST_READ_ONLY_FILE_PATH_0);

        fs.mkdirs(TEST_READ_ONLY_FOLDER_PATH);
        fs_with_auth.listStatus(TEST_READ_ONLY_FOLDER_PATH);
    }

    @Test
    public void testListUnauthorized() {
        try {
            fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
            fs_with_auth.listStatus(TEST_WRITE_ONLY_FILE_PATH_0);
        } catch (IOException e) {
            Assert.assertTrue("testListUnauthorized",e instanceof OBSAuthorizationException);
        }

        try {
            fs.mkdirs(TEST_WRITE_ONLY_FOLDER_PATH);
            fs_with_auth.listStatus(TEST_WRITE_ONLY_FOLDER_PATH);
        } catch (IOException e) {
            Assert.assertTrue("testListUnauthorized",e instanceof OBSAuthorizationException);
        }
    }

    @Test
    public void testRenameAuthorized() throws Exception {
        fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
        fs_with_auth.rename(TEST_WRITE_ONLY_FILE_PATH_0,TEST_WRITE_ONLY_FILE_PATH_1);

        fs.mkdirs(TEST_READ_ONLY_FOLDER_PATH);
        fs_with_auth.rename(TEST_WRITE_ONLY_FOLDER_PATH,TEST_READ_WRITE_FOLDER_PATH);
    }

    @Test
    public void testRenameUnauthorized() {
        try {
            fs.create(TEST_READ_ONLY_FILE_PATH_0).close();
            fs_with_auth.rename(TEST_READ_ONLY_FILE_PATH_0,TEST_WRITE_ONLY_FILE_PATH_1);
        } catch (IOException e) {
            Assert.assertTrue("testListUnauthorized",e instanceof OBSAuthorizationException);
        }

        try {
            fs.mkdirs(TEST_READ_ONLY_FOLDER_PATH);
            fs_with_auth.rename(TEST_READ_ONLY_FOLDER_PATH,TEST_WRITE_ONLY_FOLDER_PATH);
        } catch (IOException e) {
            Assert.assertTrue("testRenameUnauthorized",e instanceof OBSAuthorizationException);
        }
    }
}
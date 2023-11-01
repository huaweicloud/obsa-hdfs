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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.obs.services.ObsClient;
import com.obs.services.model.ListBucketsRequest;
import com.obs.services.model.ObsBucket;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;

public class ITestOBSFileSystem {

    private static final int EXPECTED_PORT = -1;

    private static final String EXPECTED_SCHEMA = "obs";

    private Configuration conf;

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        conf = OBSContract.getConfiguration(null);
    }

    @Test
    // 通过configuration初始化FS，功能正常
    public void testInitialization() throws IOException {
        String bucketName = conf.get(OBSTestConstants.TEST_FS_OBS_NAME);
        initializationTest(bucketName);
    }

    @Test
    public void testInitializeNotExistsBucket() throws URISyntaxException, IOException {
        String initializationUri = conf.get(OBSTestConstants.TEST_FS_OBS_NAME);
        URI uri = new URI(initializationUri);
        String scheme = uri.getScheme();
        String bucketName = uri.getAuthority();
        while (true) {
            bucketName = String.format("%s-not-exists", bucketName);
            OBSFileSystem fs = new OBSFileSystem();
            try {
                initializationUri = String.format("%s://%s", scheme, bucketName);
                fs.initialize(URI.create(initializationUri), conf);
            } catch (FileNotFoundException e) {
                assertEquals("Bucket " + bucketName + " does not exist", e.getMessage());
                return;
            }
            List<ObsBucket> buckets = getBucketList(fs.getObsClient());
            boolean isBucketInList = false;
            for (ObsBucket bucket : buckets) {
                if (bucket.getBucketName().equals(fs.getBucket())) {
                    isBucketInList = true;
                    break;
                }
            }
            if (isBucketInList) {
                continue;
            }
            assertTrue("not in bucket list, but initalize success", false);
        }
    }

    @Test
    public void testGetTrashRootV1() throws IOException {
        Path path = new Path("test");
        try (OBSFileSystem fs = OBSTestUtils.createTestFileSystem(conf)) {
            Path obsTrashRoot = fs.getTrashRoot(path);
            Path oldTrashRoot = fs.makeQualified(new Path(fs.getHomeDirectory().toUri().getPath(), ".Trash"));
            assertEquals(oldTrashRoot, obsTrashRoot);
        }
    }

    @Test
    public void testGetTrashRootV2() throws IOException {
        Path path = new Path("test");
        Configuration newConf = new Configuration(conf);
        newConf.set(OBSConstants.HDFS_TRASH_VERSION, OBSConstants.HDFS_TRASH_VERSION_V2);
        try (OBSFileSystem newFs = OBSTestUtils.createTestFileSystem(newConf)) {
            Path obsTrashRoot2 = newFs.getTrashRoot(path);
            Path ourTrashRoot =
                newFs.makeQualified(new Path(OBSConstants.DEFAULT_HDFS_TRASH_PREFIX, OBSFileSystem.getUsername()));
            assertEquals(ourTrashRoot, obsTrashRoot2);
        }
    }

    @Test
    public void testCustomTrashPrefix() throws IOException {
        Path path = new Path("test");
        Configuration newConf = new Configuration(conf);
        newConf.set(OBSConstants.HDFS_TRASH_VERSION, OBSConstants.HDFS_TRASH_VERSION_V2);
        String prefix = "/tmp/.trash";
        newConf.set(OBSConstants.HDFS_TRASH_PREFIX, prefix);
        try (OBSFileSystem newFs = OBSTestUtils.createTestFileSystem(newConf)) {
            Path obsTrashRoot2 = newFs.getTrashRoot(path);
            Path ourTrashRoot =
                    newFs.makeQualified(new Path(prefix, OBSFileSystem.getUsername()));
            assertEquals(ourTrashRoot, obsTrashRoot2);
        }
    }

    @Test
    public void testGetAllTrashRootsV1() throws IOException {
        testGetTrashRootsV1(true);
    }

    @Test
    public void testGetCurUserTrashRootsV1() throws IOException {
        testGetTrashRootsV1(false);
    }

    @Test
    public void testGetAllTrashRootsV2() throws IOException {
        testGetTrashRootsV2(true, null);
    }

    @Test
    public void testCustomPrefixGetAllTrashRootsV2() throws IOException {
        testGetTrashRootsV2(true, "/tmp/.Trash");
    }

    @Test
    public void testGetCurUserTrashRootsV2() throws IOException {
        testGetTrashRootsV2(false, null);
    }

    @Test
    public void testCustomPrefixGetCurUserTrashRootsV2() throws IOException {
        testGetTrashRootsV2(false, "/tmp/.Trash");
    }

    public void testGetTrashRootsV1(boolean allUsers) throws IOException {
        OBSFileSystem fs = OBSTestUtils.createTestFileSystem(conf);
        Path user66TrashRoot = new Path(fs.getHomeDirectory().getParent(), "user66/.Trash");
        Path curUserTrashRoot = fs.makeQualified(new Path(fs.getHomeDirectory().toUri().getPath(), ".Trash"));
        try {
            fs.mkdirs(user66TrashRoot);
            fs.mkdirs(curUserTrashRoot);
            Collection<FileStatus> trashRoots = fs.getTrashRoots(allUsers);
            if (allUsers) {
                assertTrue(containsPathInFileStatuses(trashRoots, user66TrashRoot));
                assertTrue(containsPathInFileStatuses(trashRoots, curUserTrashRoot));
            } else {
                assertEquals(1, trashRoots.size());
                assertEquals(curUserTrashRoot, trashRoots.toArray(new FileStatus[0])[0].getPath());
            }
        } finally {
            fs.delete(user66TrashRoot.getParent(), true);
            fs.delete(curUserTrashRoot.getParent(), true);
            fs.close();
        }
    }

    public void testGetTrashRootsV2(boolean allUsers, String hdfsTrashPrefix) throws IOException {
        Configuration newConf = new Configuration(conf);
        newConf.set(OBSConstants.HDFS_TRASH_VERSION, OBSConstants.HDFS_TRASH_VERSION_V2);
        if (hdfsTrashPrefix == null) {
            hdfsTrashPrefix = OBSConstants.DEFAULT_HDFS_TRASH_PREFIX;
        } else {
            newConf.set(OBSConstants.HDFS_TRASH_PREFIX, hdfsTrashPrefix);
        }

        OBSFileSystem newFs = OBSTestUtils.createTestFileSystem(newConf);
        Path ourCurUserTrashRoot = newFs.makeQualified(new Path(hdfsTrashPrefix, OBSFileSystem.getUsername()));
        Path user88TrashDir = new Path(ourCurUserTrashRoot.getParent(), "user88");
        try {
            newFs.mkdirs(user88TrashDir);
            newFs.mkdirs(ourCurUserTrashRoot);
            Collection<FileStatus> trashRoots = newFs.getTrashRoots(allUsers);
            if (allUsers) {
                assertTrue(containsPathInFileStatuses(trashRoots, user88TrashDir));
                assertTrue(containsPathInFileStatuses(trashRoots, ourCurUserTrashRoot));
            } else {
                assertEquals(1, trashRoots.size());
                assertEquals(ourCurUserTrashRoot, trashRoots.toArray(new FileStatus[0])[0].getPath());
            }
        } finally {
            newFs.delete(user88TrashDir, true);
            newFs.delete(ourCurUserTrashRoot, true);
            newFs.close();
        }
    }

    private boolean containsPathInFileStatuses(Collection<FileStatus> fileStatuses, Path path) {
        for (FileStatus status : fileStatuses) {
            if (status.getPath().equals(path)) {
                return true;
            }
        }

        return false;
    }

    private void initializationTest(String initializationUri) throws IOException {
        OBSFileSystem fs = new OBSFileSystem();
        fs.initialize(URI.create(initializationUri), conf);
        ObsClient obsTest = fs.getObsClient();
        List<ObsBucket> buckets = getBucketList(obsTest);

        boolean isBucketInList = false;
        for (ObsBucket bucket : buckets) {
            if (bucket.getBucketName().equals(fs.getBucket())) {
                isBucketInList = true;
                break;
            }
        }

        URI EXPECTED_URI = URI.create(conf.get(OBSTestConstants.TEST_FS_OBS_NAME));
        assertTrue(isBucketInList);
        assertEquals(EXPECTED_SCHEMA, fs.getScheme());
        assertEquals(EXPECTED_URI, fs.getUri());
        assertEquals(EXPECTED_PORT, fs.getDefaultPort());
    }

    private List<ObsBucket> getBucketList(ObsClient obsTest) {
        ListBucketsRequest request = new ListBucketsRequest();
        request.setQueryLocation(true);
        List<ObsBucket> buckets = obsTest.listBuckets(request);
        return buckets;
    }
}

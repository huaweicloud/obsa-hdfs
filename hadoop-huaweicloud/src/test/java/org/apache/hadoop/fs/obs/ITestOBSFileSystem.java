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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.obs.services.ObsClient;
import com.obs.services.model.*;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.obs.Constants.*;


public class ITestOBSFileSystem extends TestCase {

  public static final URI EXPECTEDURI = URI.create("obs://obs-filesystem-posix-bucket-01/");

  public void testInitialization() throws IOException {
    initializationTest("obs://obs-filesystem-posix-bucket-01/");
  }
  
  private void initializationTest(String initializationUri)
    throws IOException {
    
    OBSFileSystem fs = new OBSFileSystem();
    Configuration conf = new Configuration();
    fs.initialize(URI.create(initializationUri),conf);
    ObsClient obsTest = fs.getObsClient();
    List<ObsBucket> buckets = getBucketList(obsTest);
    String path = conf.getTrimmed(PATH_LOCAL_TEST, "");
    String bucketName = fs.getBucket();
    List<String> objectKeys = getBucketObjectKey(obsTest, bucketName);
    obsTest.putObject(bucketName, "test file",
            new File(path));
    assertEquals(buckets.get(0).getBucketName(), fs.getBucket());
    assertEquals(EXPECTEDURI, fs.getUri());
  }
  private List<ObsBucket> getBucketList(ObsClient obsTest){
    ListBucketsRequest request = new ListBucketsRequest();
    request.setQueryLocation(true);
    List<ObsBucket> buckets = obsTest.listBuckets(request);
    return  buckets;
  }

  private List<String> getBucketObjectKey(ObsClient obsTest, String bucketName){
    List<String> objectKeys = new ArrayList<String>();
    ObjectListing result = obsTest.listObjects(bucketName);
    for (ObsObject obsObject : result.getObjects())
    {
      objectKeys.add(obsObject.getObjectKey());
    }

    return  objectKeys;
  }

}

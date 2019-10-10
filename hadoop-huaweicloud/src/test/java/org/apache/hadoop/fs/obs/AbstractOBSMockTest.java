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

import static org.apache.hadoop.fs.obs.Constants.*;

import java.net.URI;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;

import org.apache.hadoop.conf.Configuration;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

/**
 * Abstract base class for OBS unit tests using a mock OBS client.
 */
public abstract class AbstractOBSMockTest {

  protected static final String BUCKET = "mock-bucket";
  protected static final ObsException NOT_FOUND;
  static {
    NOT_FOUND = new ObsException("Not Found");
    NOT_FOUND.setResponseCode(404);
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  protected OBSFileSystem fs;
  protected ObsClient obsClient;

  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(OBS_CLIENT_FACTORY_IMPL, MockOBSClientFactory.class,
        ObsClientFactory.class);
    fs = new OBSFileSystem();
    URI uri = URI.create(FS_OBS + "://" + BUCKET);
    fs.initialize(uri, conf);
    obsClient = fs.getObsClient();
  }

  @After
  public void teardown() throws Exception {
    if (fs != null) {
      fs.close();
    }
  }

}

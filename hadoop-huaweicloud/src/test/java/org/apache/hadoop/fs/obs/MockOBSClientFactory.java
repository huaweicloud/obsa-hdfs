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

import static org.mockito.Mockito.*;

import java.net.URI;

import com.obs.services.ObsClient;

/**
 * An {@link OBSClientFactory} that returns Mockito mocks of the
 * interface suitable for unit testing.
 */
public class MockOBSClientFactory implements OBSClientFactory {

    @Override
    public ObsClient createObsClient(URI name) {
        String bucket = name.getHost();
        ObsClient obsClient = mock(ObsClient.class);
        when(obsClient.headBucket(bucket)).thenReturn(true);
        return obsClient;
    }
}

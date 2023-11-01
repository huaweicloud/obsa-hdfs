/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.obs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;
import org.apache.hadoop.fs.obs.OBSConstants;
import org.apache.hadoop.fs.obs.OBSTestConstants;
import org.apache.hadoop.fs.obs.OBSTestUtils;

/**
 * The contract of OBS: only enabled if the test bucket is provided.
 */
public class OBSContract extends AbstractBondedFSContract {

    public static final String CONTRACT_XML = "contract/obs.xml";

    public OBSContract(Configuration conf) {
        super(conf);
        //insert the base features
        addConfResource(CONTRACT_XML);
        this.setConf(getConfiguration(conf));
    }

    @Override
    public String getScheme() {
        return "obs";
    }

    @Override
    public Path getTestPath() {
        return OBSTestUtils.createTestPath(super.getTestPath());
    }

    public synchronized static boolean isContractTestEnabled() {
        Configuration conf = null;
        boolean isContractTestEnabled = true;

        if (conf == null) {
            conf = getConfiguration(null);
        }
        String fileSystem = conf.get(OBSTestConstants.TEST_FS_OBS_NAME);
        if (fileSystem == null || fileSystem.trim().length() == 0) {
            isContractTestEnabled = false;
        }
        return isContractTestEnabled;
    }

    public synchronized static Configuration getConfiguration(
        final Configuration conf) {
        Configuration newConf = conf == null ? new Configuration()
            : new Configuration(conf);
        newConf.addResource(CONTRACT_XML);
        if (!OBSTestUtils.userAk.equals("")
            && !OBSTestUtils.userSk.equals("")
            && !OBSTestUtils.endPoint.equals("")
            && !OBSTestUtils.endPoint.equals("")) {
            newConf.set(OBSConstants.ACCESS_KEY, OBSTestUtils.userAk);
            newConf.set(OBSConstants.SECRET_KEY, OBSTestUtils.userSk);
            newConf.set(OBSConstants.ENDPOINT, OBSTestUtils.endPoint);
            newConf.set(OBSTestConstants.TEST_FS_OBS_NAME,
                OBSTestUtils.bucketName);
        }

        return newConf;
    }
}

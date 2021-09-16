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

package org.apache.flink.fs.obshadoop;

import org.junit.Assume;
import org.junit.AssumptionViolatedException;

import javax.annotation.Nullable;

/**
 * Access to credentials to access OBS buckets during integration tests.
 */
public class OBSTestCredentials {

    @Nullable
    public static String OBS_TEST_BUCKET = System.getenv("IT_CASE_OBS_BUCKET");

    @Nullable
    public static String OBS_TEST_ACCESS_KEY = System.getenv("IT_CASE_OBS_ACCESS_KEY");

    @Nullable
    public static String OBS_TEST_SECRET_KEY = System.getenv("IT_CASE_OBS_SECRET_KEY");

    @Nullable
    public static String OBS_TEST_ENDPOINT = System.getenv("IT_CASE_OBS_ENDPOINT ");
    // ------------------------------------------------------------------------

    /**
     * Checks whether OBS test credentials are available in the environment variables of this JVM.
     */
    private static boolean credentialsAvailable() {
        return isNotEmpty(OBS_TEST_BUCKET)
            && isNotEmpty(OBS_TEST_ACCESS_KEY)
            && isNotEmpty(OBS_TEST_SECRET_KEY)
            && isNotEmpty(OBS_TEST_ENDPOINT);
    }

    /**
     * Checks if a String is not null and not empty.
     */
    private static boolean isNotEmpty(@Nullable String str) {
        return str != null && !str.isEmpty();
    }

    /**
     * Checks whether credentials are available in the environment variables of this JVM. If not,
     * throws an {@link AssumptionViolatedException} which causes JUnit tests to be skipped.
     */
    public static void assumeCredentialsAvailable() {
        Assume.assumeTrue(
            "No OBS credentials available in this test's environment", credentialsAvailable());
    }

    /**
     * Gets the OBS Access Key.
     *
     * <p>This method throws an exception if the key is not available. Tests should use {@link
     * #assumeCredentialsAvailable()} to skip tests when credentials are not available.
     */
    public static String getOBSAccessKey() {
        if (OBS_TEST_ACCESS_KEY != null) {
            return OBS_TEST_ACCESS_KEY;
        } else {
            throw new IllegalStateException("OBS test access key not available");
        }
    }

    /**
     * Gets the OBS Secret Key.
     *
     * <p>This method throws an exception if the key is not available. Tests should use {@link
     * #assumeCredentialsAvailable()} to skip tests when credentials are not available.
     */
    public static String getOBSSecretKey() {
        if (OBS_TEST_SECRET_KEY != null) {
            return OBS_TEST_SECRET_KEY;
        } else {
            throw new IllegalStateException("OBS test secret key not available");
        }
    }

    /**
     * Gets the OBS Secret Key.
     *
     * <p>This method throws an exception if the key is not available. Tests should use {@link
     * #assumeCredentialsAvailable()} to skip tests when credentials are not available.
     */
    public static String getOBSEndpoint() {
        if (OBS_TEST_ENDPOINT != null) {
            return OBS_TEST_ENDPOINT;
        } else {
            throw new IllegalStateException("OBS test secret key not available");
        }
    }

    /**
     * Gets the URI for the path under which all tests should put their data.
     *
     * <p>This method throws an exception if the bucket was not configured. Tests should use {@link
     * #assumeCredentialsAvailable()} to skip tests when credentials are not available.
     */
    public static String getTestBucketUri() {
        return getTestBucketUriWithScheme("obs");
    }

    /**
     * Gets the URI for the path under which all tests should put their data.
     *
     * <p>This method throws an exception if the bucket was not configured. Tests should use {@link
     * #assumeCredentialsAvailable()} to skip tests when credentials are not available.
     */
    public static String getTestBucketUriWithScheme(String scheme) {
        if (OBS_TEST_BUCKET != null) {
            return scheme + "://" + OBS_TEST_BUCKET + "/temp/";
        } else {
            throw new IllegalStateException("OBS test bucket not available");
        }
    }
}

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.hadoop.fs.obs.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * description
 *
 * @since 2021-10-13
 */
public class MockAuthorizeProvider implements AuthorizeProvider,DelegationTokenCapability {
    public static final String TEST_BASE_FOLDER = "rangertest/";
    public static final String TEST_READ_ONLY_FILE_0 = TEST_BASE_FOLDER + "readOnlyFile0";
    public static final String TEST_READ_ONLY_FILE_1 = TEST_BASE_FOLDER + "readOnlyFile1";
    public static final String TEST_READ_ONLY_FOLDER = TEST_BASE_FOLDER + "readOnlyFolder";

    public static final String TEST_WRITE_ONLY_FILE_0 = TEST_BASE_FOLDER + "writeOnlyFile0";
    public static final String TEST_WRITE_ONLY_FILE_1 = TEST_BASE_FOLDER + "writeOnlyFile1";
    public static final String TEST_WRITE_ONLY_FOLDER = TEST_BASE_FOLDER + "writeOnlyFolder";

    public static final String TEST_READ_WRITE_FILE = TEST_BASE_FOLDER + "readWriteFile";
    public static final String TEST_READ_WRITE_FOLDER = TEST_BASE_FOLDER + "readWriteFolder";

    private Set<String> readOnlyPaths = new HashSet<String>();
    private Set<String> writeOnlyPaths = new HashSet<String>();
    private Set<String> readWritePaths = new HashSet<String>();

    @Override
    public void init(Configuration conf) {
        readOnlyPaths.add(TEST_READ_ONLY_FILE_0);
        readOnlyPaths.add(TEST_READ_ONLY_FILE_1);
        readOnlyPaths.add(TEST_READ_ONLY_FOLDER);

        writeOnlyPaths.add(TEST_WRITE_ONLY_FILE_0);
        writeOnlyPaths.add(TEST_WRITE_ONLY_FILE_1);
        writeOnlyPaths.add(TEST_WRITE_ONLY_FOLDER);

        readWritePaths.add(TEST_READ_WRITE_FILE);
        readWritePaths.add(TEST_READ_WRITE_FOLDER);
    }

    @Override
    public boolean isAuthorized(String bucket, String key, AccessType action) {
        Set<String> reads = Stream.concat(readOnlyPaths.stream(), readWritePaths.stream())
            .collect(Collectors.toSet());
        Set<String> writes = Stream.concat(writeOnlyPaths.stream(), readWritePaths.stream()).collect(Collectors.toSet());
        if (action.equals(AccessType.READ) && reads.contains(key)) {
            return true;
        } else if (action.equals(AccessType.WRITE) && writes.contains(key)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String getCanonicalServiceName() {
        return null;
    }

    @Override
    public Token<?> getDelegationToken(String renewer) throws IOException {
        return null;
    }
}

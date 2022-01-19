/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.security.authorization;

public class PermissionRequest {
    private AccessType accessType;

    private String bucketName;

    private String objectKey;

    public PermissionRequest(AccessType accessType, String bucketName, String objectKey) {
        this.accessType = accessType;
        this.bucketName = bucketName;
        this.objectKey = objectKey;

    }

    public AccessType getAccessType() {
        return this.accessType;
    }

    public String getBucketName() {
        return this.bucketName;
    }

    public String getObjectKey() {
        return this.objectKey;
    }
}

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.security.sts;

public class GetSTSRequest {
    private String bucketName;

    private String region;

    private String allowPrefix;

    public String getBucketName() {
        return this.bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getAllowPrefix() {
        return this.allowPrefix;
    }

    public void setAllowPrefix(String allowPrefix) {
        this.allowPrefix = allowPrefix;
    }
}

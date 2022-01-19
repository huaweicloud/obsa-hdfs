/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.authorize;

import com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.obs.security.authorization.PermissionRequest;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * RangerAuthorizer utilize RangerBasePlugin conduct check permission
 */
public class RangerAuthorizer implements Authorizer {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizer.class);

    private RangerBasePlugin rangerObsPlugin = null;

    @Override
    public void init(Configuration conf) {
        String obsServiceName = conf.get("ranger.plugin.obs.service.name", "obs");

        this.rangerObsPlugin = new RangerBasePlugin("obs", obsServiceName);
        this.rangerObsPlugin.setResultProcessor(new RangerDefaultAuditHandler());
        this.rangerObsPlugin.init();
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        if (this.rangerObsPlugin != null) {
            this.rangerObsPlugin.cleanup();
            this.rangerObsPlugin = null;
        }
    }

    @Override
    public boolean checkPermission(PermissionRequest permissionReq, UserGroupInformation ugi) {
        RangerAccessRequestImpl rangerAccessReq = new RangerAccessRequestImpl();
        rangerAccessReq.setUser(ugi.getShortUserName());
        rangerAccessReq.setUserGroups(Sets.newHashSet(ugi.getGroupNames()));
        rangerAccessReq.setAccessTime(new Date());
        rangerAccessReq.setAccessType(permissionReq.getAccessType().toString().toLowerCase());
        RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();

        rangerResource.setValue("bucket", permissionReq.getBucketName());
        rangerResource.setValue("path", permissionReq.getObjectKey());
        rangerAccessReq.setResource(rangerResource);
        RangerAccessResult result = null;
        result = this.rangerObsPlugin.isAccessAllowed(rangerAccessReq);
        if (result == null) {
            LOG.warn("check permission result is null");
            return false;
        } else if (result.getIsAllowed()) {
            return true;
        } else {
            return false;
        }
    }
}

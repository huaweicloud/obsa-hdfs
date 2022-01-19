/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs;

import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerObsService extends RangerBaseService {

    @Override
    public Map<String, Object> validateConfig() throws Exception {
        return this.buildResponseDataMap(true, "connection test successful", "connection test successful");
    }

    private Map<String, Object> buildResponseDataMap(boolean connectSuccess, String message, String description) {
        Map<String, Object> responseData = new HashMap();
        BaseClient.generateResponseDataMap(connectSuccess, message, description, (Long) null, (String) null,
            responseData);
        return responseData;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext resourceLookupContext) throws Exception {
        List<String> resourceList = new ArrayList();
        String userInput = resourceLookupContext.getUserInput();
        resourceList.add(userInput);
        return resourceList;
    }
}

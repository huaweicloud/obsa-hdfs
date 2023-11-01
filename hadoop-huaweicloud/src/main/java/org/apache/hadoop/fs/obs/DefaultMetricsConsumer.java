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

package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Default class for consuming metrics.
 */
class DefaultMetricsConsumer implements BasicMetricsConsumer {

    /**
     * Class logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricsConsumer.class);

    /**
     * URI of the FileSystem instance.
     */
    private URI uri;

    /**
     * Configuration of the FileSystem instance.
     */
    private Configuration conf;

    /**
     * Default metrics consumer that prints debug logs.
     *
     * @param uriName URI of the owner FileSystem
     */
    DefaultMetricsConsumer(final URI uriName, final Configuration configuration) {
        this.uri = uriName;
        this.conf = configuration;
        LOG.debug("DefaultMetricsConsumer with URI [{}] and " + "Configuration[{}]", this.uri, this.conf);
    }

    /**
     * Put metrics to the consumer.
     *
     * @param metricRecord metric record to be consumed
     * @return true for success, or false for failure
     */
    @Override
    public boolean putMetrics(MetricRecord metricRecord) {
        if (LOG.isDebugEnabled()) {
            if (metricRecord.getKind().equals(MetricKind.normal)) {
                LOG.debug("[Metrics]: url[{}], action [{}], kind[{}], costTime[{}] ", this.uri,
                        metricRecord.getObsOperateAction(), metricRecord.getKind(), metricRecord.getCostTime());
            }else {
                LOG.debug("[Metrics]: url[{}], action [{}], kind[{}], exception[{}] ", this.uri,
                        metricRecord.getObsOperateAction(), metricRecord.getKind(), metricRecord.getExceptionIns());
            }

        }
        return true;
    }

    @Override
    public void close() {
    }
}

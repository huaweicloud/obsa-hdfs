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

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface class for consuming metrics.
 */
public interface BasicMetricsConsumer extends Closeable {

    enum MetricKind {
        normal,abnormal
    }

    class MetricRecord {
        /**
         * Operation name, such as listStatus.
         */
        private OBSOperateAction obsOperateAction;

        /**
         * Operation cost time in ms.
         */
        private long costTime;
        /**
         * Obs detail error msg
         */
        private Exception exception;

        /**
         *  normal logic or exception logic
         */
        private MetricKind kind;


        //正常: kind:指示是什么性能追踪类信息还是异常；opName 操作接口；
        public MetricRecord(OBSOperateAction opName, long costTime, MetricKind kind) {
            this.obsOperateAction = opName;
            this.costTime = costTime;
            this.kind = kind;
        }

        //异常:opName 操作类型；  opDetail 代表obs错误码
        public MetricRecord(OBSOperateAction opName, Exception exception, MetricKind kind) {
            this.obsOperateAction = opName;
            this.exception = exception;
            this.kind = kind;
        }

        public OBSOperateAction getObsOperateAction() {
            return obsOperateAction;
        }

        public long getCostTime() {
            return costTime;
        }

        //获取异常实例
        public Exception getExceptionIns() {
            return exception;
        }

        public MetricKind getKind() {
            return kind;
        }

        public void setKind(MetricKind kind) {
            this.kind = kind;
        }

        @Override
        public String toString() {
            return "MetricRecord{" + "opName='" + obsOperateAction + ", costTime=" + costTime
                + ", exception=" + exception + ",kind=" + kind + '}';
        }

    }

    /**
     * Put metrics to the consumer.
     *
     * @param metricRecord metric record to be consumed
     * @return true for success, or false for failure
     */
    boolean putMetrics(MetricRecord metricRecord);

    @Override
    void close() throws IOException;
}

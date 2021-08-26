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

    class MetricRecord {
        /**
         * Operation name, such as listStatus.
         */
        private String opName;

        /**
         * Operation result: true for success, or false for failure.
         */
        private boolean success;

        /**
         * Operation cost time in ms.
         */
        private long costTime;

        private String opType;

        //opName
        public static final String READ = "read";

        public static final String CLOSE = "close";

        public static final String READFULLY = "readFully";

        static final String HFLUSH = "hflush";

        static final String WRITE = "write";

        static final String CREATE = "create";

        static final String CREATE_NR = "createNonRecursive";

        static final String APPEND = "append";

        static final String RENAME = "rename";

        static final String DELETE = "delete";

        static final String LIST_STATUS = "listStatus";

        static final String MKDIRS = "mkdirs";

        static final String GET_FILE_STATUS = "getFileStatus";

        static final String GET_CONTENT_SUMMARY = "getContentSummary";

        static final String COPYFROMLOCAL = "copyFromLocalFile";

        static final String LIST_FILES = "listFiles";

        static final String LIST_LOCATED_STS = "listLocatedStatus";

        static final String OPEN = "open";

        //opType
        public static final String ONEBYTE = "1byte";

        public static final String BYTEBUF = "byteBuf";

        public static final String INPUT = "input";

        public static final String RANDOM = "random";

        public static final String SEQ = "seq";

        static final String OUTPUT = "output";

        static final String FLAGS = "flags";

        static final String NONRECURSIVE = "nonrecursive";

        static final String RECURSIVE = "recursive";

        static final String FS = "fs";

        static final String OVERWRITE = "overwrite";

        public MetricRecord(String opType, String opName, boolean success, long costTime) {
            this.opName = opName;
            this.opType = opType;
            this.success = success;
            this.costTime = costTime;
        }

        public String getOpName() {
            return opName;
        }

        public boolean isSuccess() {
            return success;
        }

        public long getCostTime() {
            return costTime;
        }

        public String getOpType() {
            return opType;
        }

        @Override
        public String toString() {
            return "MetricRecord{" + "opName='" + opName + ", success=" + success + ", costTime=" + costTime
                + ", opType=" + opType + '}';
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

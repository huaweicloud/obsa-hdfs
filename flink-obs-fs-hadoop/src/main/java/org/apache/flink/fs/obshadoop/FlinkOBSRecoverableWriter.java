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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.UUID;

/**
 * @Author:l30002155 @Email:lixianwei6@huawei.com @Date:2020/9/15 9:36
 */
public class FlinkOBSRecoverableWriter implements RecoverableWriter {

    /**
     * The Hadoop file system on which the writer operates.
     */
    private final FileSystem fs;

    /**
     * Creates a new Recoverable writer.
     *
     * @param fs The Hadoop file system on which the writer operates.
     */
    public FlinkOBSRecoverableWriter(final FileSystem fs) {
        this.fs = Preconditions.checkNotNull(fs);

        // This writer is only supported on a subset of file systems
        if (!"obs".equalsIgnoreCase(fs.getScheme())) {
            throw new UnsupportedOperationException(
                "Recoverable writers on OBS are only supported for OBS");
        }
    }

    @Override
    public RecoverableFsDataOutputStream open(Path filePath) throws IOException {
        final org.apache.hadoop.fs.Path targetFile = FlinkOBSFileSystem.toHadoopPath(filePath);
        final org.apache.hadoop.fs.Path tempFile = generateStagingTempFilePath(fs, targetFile);
        return new FlinkOBSRecoverableFsDataOutputStream(fs, targetFile, tempFile);
    }

    @Override
    public RecoverableFsDataOutputStream recover(ResumeRecoverable recoverable) throws IOException {
        if (recoverable instanceof FlinkOBSFsRecoverable) {
            return new FlinkOBSRecoverableFsDataOutputStream(
                fs, (FlinkOBSFsRecoverable) recoverable);
        } else {
            throw new IllegalArgumentException(
                "OBS File System cannot recover a recoverable for another "
                    + "file system: "
                    + recoverable);
        }
    }

    @Override
    public boolean requiresCleanupOfRecoverableState() {
        return false;
    }

    @Override
    public boolean cleanupRecoverableState(ResumeRecoverable resumable) throws IOException {
        return false;
    }

    @Override
    public RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable recoverable)
        throws IOException {
        if (recoverable instanceof FlinkOBSFsRecoverable) {
            return new FlinkOBSRecoverableFsDataOutputStream.OBSFsCommitter(
                fs, (FlinkOBSFsRecoverable) recoverable);
        } else {
            throw new IllegalArgumentException(
                "OBS File System  cannot recover a recoverable for another "
                    + "file system: "
                    + recoverable);
        }
    }

    @Override
    public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
        @SuppressWarnings("unchecked")
        SimpleVersionedSerializer<CommitRecoverable> typedSerializer =
            (SimpleVersionedSerializer<CommitRecoverable>)
                (SimpleVersionedSerializer<?>) FlinkOBSRecoverableSerializer.INSTANCE;

        return typedSerializer;
    }

    @Override
    public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
        @SuppressWarnings("unchecked")
        SimpleVersionedSerializer<ResumeRecoverable> typedSerializer =
            (SimpleVersionedSerializer<ResumeRecoverable>)
                (SimpleVersionedSerializer<?>) FlinkOBSRecoverableSerializer.INSTANCE;

        return typedSerializer;
    }

    @Override
    public boolean supportsResume() {
        return true;
    }

    @VisibleForTesting
    static org.apache.hadoop.fs.Path generateStagingTempFilePath(
        FileSystem fs, org.apache.hadoop.fs.Path targetFile)
        throws IOException {

        Preconditions.checkArgument(targetFile.isAbsolute(), "targetFile must be absolute");

        final org.apache.hadoop.fs.Path parent = targetFile.getParent();
        final String name = targetFile.getName();

        Preconditions.checkArgument(parent != null, "targetFile must not be the root directory");

        while (true) {
            org.apache.hadoop.fs.Path candidate =
                new org.apache.hadoop.fs.Path(
                    parent, "." + name + ".inprogress." + UUID.randomUUID().toString());
            if (!fs.exists(candidate)) {
                return candidate;
            }
        }
    }
}

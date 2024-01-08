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

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.obs.OBSAlreadyBeingCreatedException;
import org.apache.hadoop.fs.obs.OBSFileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @Author:l30002155 @Email:lixianwei6@huawei.com @Date:2020/9/15 9:41
 */
public class FlinkOBSRecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {

    private final FileSystem fs;

    private final Path targetFile;

    private final Path tempFile;

    private final FSDataOutputStream out;

    FlinkOBSRecoverableFsDataOutputStream(FileSystem fs, Path targetFile, Path tempFile)
        throws IOException {

        this.fs = Preconditions.checkNotNull(fs);
        this.targetFile = Preconditions.checkNotNull(targetFile);
        this.tempFile = Preconditions.checkNotNull(tempFile);
        this.out = fs.create(tempFile);
    }

    FlinkOBSRecoverableFsDataOutputStream(
        final FileSystem fs, final FlinkOBSFsRecoverable recoverable) throws IOException {
        this.fs = Preconditions.checkNotNull(fs);
        this.targetFile = Preconditions.checkNotNull(recoverable.targetFile());
        this.tempFile = Preconditions.checkNotNull(recoverable.tempFile());

        safelyTruncateFile((OBSFileSystem) fs, tempFile, recoverable);

        out = fs.append(tempFile);

        // sanity check
        long pos = out.getPos();
        if (pos != recoverable.offset()) {
            IOUtils.closeQuietly(out);
            throw new IOException(
                "Truncate failed: "
                    + tempFile
                    + " (requested="
                    + recoverable.offset()
                    + " ,size="
                    + pos
                    + ')');
        }
    }

    static class OBSFsCommitter implements Committer {
        private final FileSystem fs;

        private final FlinkOBSFsRecoverable recoverable;

        OBSFsCommitter(final FileSystem fs, final FlinkOBSFsRecoverable recoverable) {
            this.fs = Preconditions.checkNotNull(fs);
            this.recoverable = Preconditions.checkNotNull(recoverable);
        }

        @Override
        public void commit() throws IOException {
            final Path src = recoverable.tempFile();
            final Path dest = recoverable.targetFile();
            final long expectedLength = recoverable.offset();

            final FileStatus srcStatus;
            try {
                srcStatus = fs.getFileStatus(src);
            } catch (IOException e) {
                throw new IOException("Cannot clean commit: Staging file does not exist.");
            }

            if (srcStatus.getLen() != expectedLength) {
                throw new IOException("Cannot clean commit: File has trailing junk data.");
            }

            try {
                fs.rename(src, dest);
            } catch (IOException e) {
                throw new IOException(
                    "Committing file by rename failed: " + src + " to " + dest, e);
            }
        }

        @Override
        public void commitAfterRecovery() throws IOException {
            final Path src = recoverable.tempFile();
            final Path dest = recoverable.targetFile();
            final long expectedLength = recoverable.offset();

            FileStatus srcStatus = null;
            try {
                srcStatus = fs.getFileStatus(src);
            } catch (FileNotFoundException e) {
            } catch (IOException e) {
                throw new IOException(
                    "Committing during recovery failed: Could not access status of source file.");
            }

            if (srcStatus != null) {
                if (srcStatus.getLen() > expectedLength) {
                    safelyTruncateFile((OBSFileSystem) fs, src, recoverable);
                }

                try {
                    fs.rename(src, dest);
                } catch (IOException e) {
                    throw new IOException(
                        "Committing file by rename failed: " + src + " to " + dest, e);
                }
            }
        }

        @Override
        public RecoverableWriter.CommitRecoverable getRecoverable() {
            return recoverable;
        }
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        out.hflush();
    }

    @Override
    public void sync() throws IOException {
        out.hflush();
        out.hsync();
    }

    @Override
    public long getPos() throws IOException {
        return out.getPos();
    }

    @Override
    public RecoverableWriter.ResumeRecoverable persist() throws IOException {
        sync();
        return new FlinkOBSFsRecoverable(targetFile, tempFile, getPos());
    }

    @Override
    public Committer closeForCommit() throws IOException {
        final long pos = getPos();
        close();
        return new FlinkOBSRecoverableFsDataOutputStream.OBSFsCommitter(
            fs, new FlinkOBSFsRecoverable(targetFile, tempFile, pos));
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    private static void safelyTruncateFile(
        final OBSFileSystem fileSystem,
        final Path path,
        final FlinkOBSFsRecoverable recoverable)
        throws IOException {

        // fileSystem.truncate(path,recoverable.offset());
        while (true) {
            try {
                fileSystem.truncate(path, recoverable.offset());
            } catch (OBSAlreadyBeingCreatedException e) {
                try {
                    Thread.sleep(500L);
                } catch (InterruptedException ex) {
                    throw e;
                }
                continue;
            }
            break;
        }
    }
}

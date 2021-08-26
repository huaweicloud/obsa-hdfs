package org.apache.hadoop.fs.obs.input;

import com.google.common.util.concurrent.ListeningExecutorService;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.obs.OBSFileSystem;

/**
 * read Policy Factory
 *
 * @since 2021-03-10
 */
public interface InputPolicyFactory {
    FSInputStream create(final OBSFileSystem obsFileSystem, String bucket, String key, Long contentLength,
        FileSystem.Statistics statistics, ListeningExecutorService boundedThreadPool);
}

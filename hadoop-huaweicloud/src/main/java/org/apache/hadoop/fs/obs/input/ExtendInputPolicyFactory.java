package org.apache.hadoop.fs.obs.input;

import com.google.common.util.concurrent.ListeningExecutorService;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.obs.OBSCommonUtils;
import org.apache.hadoop.fs.obs.OBSConstants;
import org.apache.hadoop.fs.obs.OBSFileStatus;
import org.apache.hadoop.fs.obs.OBSFileSystem;
import org.apache.hadoop.fs.obs.SemaphoredDelegatingExecutor;

import java.io.IOException;

/**
 * 功能描述
 *
 * @since 2021-03-10
 */
public class ExtendInputPolicyFactory implements InputPolicyFactory {

    /**
     * Create a temp file and a {@link } instance to manage it.
     *
     * @param bucket block index
     * @param key    limit of the block.
     * @return the new block
     * @throws IOException IO problems
     */
    @Override
    public FSInputStream create(final OBSFileSystem obsFileSystem, String bucket, String key, Long contentLength,
        FileSystem.Statistics statistics, ListeningExecutorService boundedThreadPool, OBSFileStatus fileStatus) {

        int maxReadAhead = OBSCommonUtils.intOption(obsFileSystem.getConf(), OBSConstants.READAHEAD_MAX_NUM,
            OBSConstants.DEFAULT_READAHEAD_MAX_NUM, 1);

        return new OBSExtendInputStream(obsFileSystem, obsFileSystem.getConf(),
            new SemaphoredDelegatingExecutor(boundedThreadPool, maxReadAhead, true), bucket, key, contentLength,
            statistics);
    }
}

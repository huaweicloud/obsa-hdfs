package org.apache.hadoop.fs.obs.input;

import com.google.common.util.concurrent.ListeningExecutorService;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.obs.OBSCommonUtils;
import org.apache.hadoop.fs.obs.OBSConstants;
import org.apache.hadoop.fs.obs.OBSFileStatus;
import org.apache.hadoop.fs.obs.OBSFileSystem;

/**
 * 功能描述
 *
 * @since 2021-05-19
 */
public class MemArtsCCInputPolicyFactory implements InputPolicyFactory {

    @Override
    public FSInputStream create(final OBSFileSystem obsFileSystem, String bucket, String key, Long contentLength,
        FileSystem.Statistics statistics, ListeningExecutorService boundedThreadPool, OBSFileStatus fileStatus) {
        long readAheadRange = OBSCommonUtils.longBytesOption(obsFileSystem.getConf(), OBSConstants.READAHEAD_RANGE,
            OBSConstants.DEFAULT_READAHEAD_RANGE, 0);
        long memartsccReadAheadRangeValue = OBSCommonUtils.longBytesOption(obsFileSystem.getConf(),
            OBSConstants.MEMARTSCC_READAHEAD_RANGE, OBSConstants.DEFAULT_MEMARTSCC_READAHEAD_RANGE, 0);
        return new OBSMemArtsCCInputStream(bucket, key, contentLength, statistics, readAheadRange,
            memartsccReadAheadRangeValue, obsFileSystem, fileStatus);
    }
}

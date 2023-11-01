package org.apache.hadoop.fs.obs.memartscc;

/**
 * 功能描述
 *
 * @since 2021-05-24
 */
public class CcGetShardParam {
    long start;

    long end;

    String bucketName;

    boolean enablePosix;

    public String objectKey;

    ObjectShard[] ObjectShard;

    int allocShardNum;

    int validShardNum;

    public CcGetShardParam(long start, long end, String bucketName, boolean enablePosix, String objectKey,
        ObjectShard[] ObjectShard, int allocShardNum, int validShardNum) {
        this.start = start;
        this.end = end;
        this.bucketName = bucketName;
        this.enablePosix = enablePosix;
        this.objectKey = objectKey;
        this.ObjectShard = ObjectShard;
        this.allocShardNum = allocShardNum;
        this.validShardNum = validShardNum;
    }

    public int getValidShardNum() {
        return validShardNum;
    }

    public ObjectShard[] getObjectShard() {
        return ObjectShard;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public String getBucketName() {
        return bucketName;
    }

    public boolean isEnablePosix() {
        return enablePosix;
    }

    public int getAllocShardNum() {
        return allocShardNum;
    }

}

package org.apache.hadoop.fs.obs.memartscc;

/**
 * 功能描述
 *
 * @since 2021-05-20
 */
public class ObjectInfo {
    public String objectKey;

    public long modifyTime;

    public boolean isConsistencyCheck;

    public ObjectInfo(String objectKey, long modifyTime, boolean isConsistencyCheck) {
        this.objectKey = objectKey;
        this.modifyTime = modifyTime;
        this.isConsistencyCheck = isConsistencyCheck;
    }
}

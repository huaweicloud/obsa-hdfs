package org.apache.hadoop.fs.obs.memartscc;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ICache {
    int init(String config, String otherInfo) throws IOException;

    int read(ReadParam readParam, ByteBuffer buf, byte[] dt) throws IOException;

    int getObjectShardInfo(CcGetShardParam ccGetShardParam);

    byte[] getDT();

    // originalTraffic: Q
    // applicationTraffic: Q'
    // hitTraffic: Q2
    // missTraffic: Q1
    void reportReadStatistics(long originalTraffic, long applicationTraffic, long hitTraffic, long missTraffic);

    void close();

    class ReadParam {
        public long offset;
        public long length;
        public long prefetchStart;
        public long prefetchEnd;
        public boolean isPrefetch;
        public boolean isConsistencyCheck;
        public boolean isFileLayout;    // 特性暂未启用，默认false
        public BucketContext bucketCtx;
        public ObjectAttr objectAttr;

        public ReadParam(long offset, long length, long prefetchStart, long prefetchEnd,
                         boolean isPrefetch, boolean isConsistencyCheck, boolean isFileLayout,
                         BucketContext bucketCtx, ObjectAttr objectAttr) {
            this.offset = offset;
            this.length = length;
            this.prefetchStart = prefetchStart;
            this.prefetchEnd = prefetchEnd;
            this.isPrefetch = isPrefetch;
            this.isConsistencyCheck = isConsistencyCheck;
            this.isFileLayout = isFileLayout;
            this.bucketCtx = bucketCtx;
            this.objectAttr = objectAttr;
        }
    }

    class BucketContext {
        public String ak;
        public String sk;
        public String token;
        public String endpoint;
        public String bucketName;
        public boolean enablePosix;

        public BucketContext(String ak, String sk, String token, String endpoint, String bucketName, boolean enablePosix) {
            this.ak = ak;
            this.sk = sk;
            this.token = token;
            this.endpoint = endpoint;
            this.bucketName = bucketName;
            this.enablePosix = enablePosix;
        }
    }

    class ObjectAttr {
        public String name;
        public String etag;
        public long mtime;

        public ObjectAttr(String name, String etag, long mtime) {
            this.name = name;
            this.etag = etag;
            this.mtime = mtime;
        }
    }
}

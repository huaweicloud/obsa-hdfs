package org.apache.hadoop.fs.obs.mock;

import com.obs.services.ObsClient;
import com.obs.services.model.GetObjectRequest;
import com.obs.services.model.ObsObject;

import org.apache.hadoop.fs.obs.OBSFileSystem;
import org.apache.hadoop.fs.obs.TrafficStatistics;
import org.apache.hadoop.fs.obs.memartscc.CcGetShardParam;
import org.apache.hadoop.fs.obs.memartscc.MemArtsCCClient;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class MockMemArtsCCClient extends MemArtsCCClient {
    private OBSFileSystem fs;

    // private Configuration conf;

    private boolean initFail;

    //    private OBSInputStream underlyingStream;

    private long bytesReaded = 0;

    String ak;

    String sk;

    String token;

    String endpoint;

    String bucket;

    boolean enablePosix;

    boolean nextIOException;

    boolean nextEOFException;

    boolean nextCacheMiss;

    private class StatisticsTotal {
        long originalTraffic;

        long applicationTraffic;

        long missTraffic;

        long hitTraffic;
    }

    StatisticsTotal statisticsTotal = new StatisticsTotal();

    public MockMemArtsCCClient(final OBSFileSystem obsFileSystem, boolean initFail, String bucket) {
        super("", true); // nouse
        this.fs = obsFileSystem;
        this.initFail = initFail;
        this.bucket = bucket;
        MockMemArtsCCClient.bufferPool.initialize(128, 1024*1024);
    }

    @Override
    public int init(String config, String otherInfo) {
        if (initFail) {
            return -1;
        }
        return 0;
    }

    @Override
    public int read(boolean isPrefetch, long prefetchStart, long prefetchEnd, ByteBuffer buf, long offset, long len,
        String objectKey, long modifyTime, String etag, boolean isConsistencyCheck) throws EOFException, IOException {
        if (nextReadCacheMiss()) {
            return CCREAD_RETCODE_CACHEMISS;
        }
        if (nextReadThrowEOFException()) {
            throw new EOFException("mock ccread EOFException");
        }
        if (nextReadThrowIOException()) {
            throw new IOException("mock ccread IOException");
        }
        if (len == 0) {
            return 0;
        }
        GetObjectRequest req = new GetObjectRequest();
        req.setBucketName(this.bucket);
        req.setObjectKey(objectKey);
        req.setRangeStart(offset);
        req.setRangeEnd(offset + len - 1);
        ObsClient client = this.fs.getObsClient();
        ObsObject obj = client.getObject(req);
        InputStream is = obj.getObjectContent();
        int bread = 0;
        int off = 0;
        byte[] tmpBuf = new byte[(int)len];
        int ret = is.read(tmpBuf, off, (int) len);
        this.bytesReaded += ret;
        bread += ret;
        off += ret;
        while (ret > 0) {
            ret = is.read(tmpBuf, off, (int) len - off);
            off += ret;
            bread += ret;
        }
        is.close();
        buf.position(0);
        buf.limit(bread);
        buf.put(tmpBuf, 0, bread);
        return bread;
    }

    @Override
    public int getObjectShardInfo(CcGetShardParam ccGetShardParam) {
        throw new UnsupportedOperationException("un-implemented mock ccGetObjectShardInfo");
    }

    @Override
    public void close() {

    }

    @Override
    public void reportReadStatistics(TrafficStatistics trafficStatistics) {
        long Q = trafficStatistics.getStatistics(TrafficStatistics.TrafficType.Q);
        long QDot = trafficStatistics.getStatistics(TrafficStatistics.TrafficType.QDot);
        long Q2 = trafficStatistics.getStatistics(TrafficStatistics.TrafficType.Q2);
        long Q1 = trafficStatistics.getStatistics(TrafficStatistics.TrafficType.Q1);

        statisticsTotal.originalTraffic += Q;
        statisticsTotal.applicationTraffic += QDot;
        statisticsTotal.hitTraffic += Q2;
        statisticsTotal.missTraffic += Q1;
    }

    public void printTotalStatistics() {
        System.out.printf("Total: Q:%d Q`:%d Q2:%d Q1:%d\n",
        statisticsTotal.originalTraffic,
        statisticsTotal.applicationTraffic,
        statisticsTotal.hitTraffic,
        statisticsTotal.missTraffic);
    }

    public boolean nextReadThrowIOException() {
        boolean ret = this.nextIOException;
        this.nextEOFException = false;
        return ret;
    }

    public boolean nextReadThrowEOFException() {
        boolean ret = this.nextEOFException;
        this.nextEOFException = false;
        return ret;
    }

    public boolean nextReadCacheMiss() {
        boolean ret = this.nextCacheMiss;
        this.nextCacheMiss = false;
        return ret;
    }

    public void setNextCCReadThrowIOException() {
        this.nextIOException = true;
    }

    public void setNextCCReadThrowEOFException() {
        this.nextEOFException = true;
    }

    public void setNextCCReadReturnCacheMiss() {
        this.nextCacheMiss = true;
    }

    public static final int CCREAD_RETCODE_CACHEMISS = -100;
}

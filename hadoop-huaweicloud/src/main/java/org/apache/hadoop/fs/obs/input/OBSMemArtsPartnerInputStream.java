package org.apache.hadoop.fs.obs.input;

import com.obs.services.ObsClient;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.obs.OBSFileSystem;
import org.apache.hadoop.fs.obs.TrafficStatistics;

import java.io.IOException;

public class OBSMemArtsPartnerInputStream extends OBSInputStream {
    private OBSMemArtsCCInputStream mistream;

    private TrafficStatistics trafficStatistics;

    OBSMemArtsPartnerInputStream(
            String bucketName,
            String bucketKey,
            long fileStatusLength,
            ObsClient obsClient,
            FileSystem.Statistics stats,
            long readAheadRangeValue,
            OBSFileSystem obsFileSystem,
            OBSMemArtsCCInputStream mistream) {
        super(bucketName, bucketKey, fileStatusLength, obsClient, stats, readAheadRangeValue, obsFileSystem);
        this.mistream = mistream;
    }

    public void setTrafficStaticsClass(TrafficStatistics instance) {
        trafficStatistics = instance;
    }

    private void increaseMissTraffic(long value) {
        if (trafficStatistics != null) {
            trafficStatistics.increase(value, TrafficStatistics.TrafficType.Q1);
        }
    }

    @Override
    protected synchronized void reopen(final String reason, final long targetPos, final long length) throws IOException {
        /**
         * reopen() in New state should call super reopen directly
         */
        if (this.mistream.getState() == OBSMemArtsCCInputStream.State.NEW) {
            super.reopen(reason, targetPos, length);
            long readFromOBS = calculateOBSTraffic(targetPos, length);
            increaseMissTraffic(readFromOBS);
        }

        /**
         * oread reopen
         */
        if (this.mistream.getState() == OBSMemArtsCCInputStream.State.OREAD) {
            /**
             * based on the implementation of OBSInputStream
             * when reopen() occurred，byteRead must be 0,
             * thus, the caller need not to consider the
             * intermediate state of reading a fractional of data.
             * Then just close the wrapper stream and throw the state transfer signal.
             */
            closeStream(reason, length);
            throw new OReadToMReadTransitionException("oread reopen(), transit to mread, origin reason: " + reason, targetPos, length);
        }

        /**
         * escape from MemArtsCC, reopen the OBSInputStream
          */
        if (this.mistream.getState() == OBSMemArtsCCInputStream.State.MREAD) {
            super.reopen(reason, targetPos, length);
            long readFromOBS = calculateOBSTraffic(targetPos, length);
            increaseMissTraffic(readFromOBS);
        }
    }

    public static class OReadToMReadTransitionException extends RuntimeException {
        static final long serialVersionUID = 5364319876219655679L;

        public long getTargetPos() {
            return targetPos;
        }

        public long getLen() {
            return len;
        }

        private long targetPos;
        private long len;

        public OReadToMReadTransitionException(String msg, long targetPos, long len) {
            super(msg);
            this.len = len;
            this.targetPos = targetPos;
        }

    }
}

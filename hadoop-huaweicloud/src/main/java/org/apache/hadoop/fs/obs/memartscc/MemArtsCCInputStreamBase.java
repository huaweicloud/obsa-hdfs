package org.apache.hadoop.fs.obs.memartscc;

import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.obs.TrafficStatistics;

import java.io.InputStream;

public abstract class MemArtsCCInputStreamBase extends InputStream implements Seekable, CanSetReadahead {
    protected TrafficStatistics trafficStatistics;

    public void setTrafficStaticsClass(TrafficStatistics instance) {
        trafficStatistics = instance;
    }

    protected void increaseHitTrafficTraffic(long value) {
        if (trafficStatistics != null) {
            trafficStatistics.increase(value, TrafficStatistics.TrafficType.Q2);
        }
    }
}

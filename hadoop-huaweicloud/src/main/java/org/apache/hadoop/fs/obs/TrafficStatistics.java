package org.apache.hadoop.fs.obs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;

public class TrafficStatistics {
    // originalTraffic: Q
    private final AtomicLong originalTraffic = new AtomicLong();

    /**
     * ApplicationTraffic: Q`.
     * <p> In fact Q` = Q1 + Q2. One should not operate the value of Q` directly.
     * <p> The value of Q` shall only be determined as Q1 + Q2.
     */
    private final AtomicLong applicationTraffic = new AtomicLong();

    /**
     * missTraffic: Q1
     */
    private final AtomicLong missTraffic = new AtomicLong();

    /**
     * hitTraffic: Q2
     */
    private final AtomicLong hitTraffic = new AtomicLong();

    private static final Logger LOG = LoggerFactory.getLogger(TrafficStatistics.class);

    public enum TrafficType {
        Q,
        QDot,
        Q1,
        Q2
    }

    public void increase(long val, TrafficType type) {
        if (val > 0) {
            long now;
            switch (type) {
                case Q:
                    now = originalTraffic.updateAndGet(addWithinLimit(val));
                    LOG.debug("originalTraffic(Q) added {}, now {}.", val, now);
                    break;
                case Q1:
                    now = missTraffic.updateAndGet(addWithinLimit(val));
                    LOG.debug("missTraffic(Q1) added {}, now {}.", val, now);
                    break;
                case Q2:
                    now = hitTraffic.updateAndGet(addWithinLimit(val));
                    LOG.debug("hitTraffic(Q2) added {}, now {}.", val, now);
                    break;
                default:
                    LOG.error("Wrong type of TrafficType, val {}.", val);
            }
        }
    }

    private LongUnaryOperator addWithinLimit(long val) {
        return (x) -> {
            if (x >= Long.MAX_VALUE) {
                return 0;
            } else {
                return x + val;
            }
        };
    }

    public long getStatistics(TrafficType type) {
        switch (type) {
            case Q:
                return originalTraffic.get();
            case QDot:
                return missTraffic.get() + hitTraffic.get();
            case Q1:
                return missTraffic.get();
            case Q2:
                return hitTraffic.get();
            default:
                LOG.error("Wrong type of TrafficType.");
                return -1;
        }
    }

    public void clearStatistics() {
        originalTraffic.getAndSet(0);
        applicationTraffic.getAndSet(0);
        missTraffic.getAndSet(0);
        hitTraffic.getAndSet(0);
        LOG.debug("Cleared all traffic statistics.");
    }
}

package org.apache.hadoop.fs.obs;

import org.apache.hadoop.fs.obs.memartscc.MemArtsCCClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.fs.obs.BlockingThreadPoolExecutorService.newDaemonThreadFactory;

public class TrafficStatisticsReporter {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficStatisticsReporter.class);

    private final TrafficStatistics trafficStatistics;

    private final MemArtsCCClient memArtsCCClient;

    /**
     * Time interval of the report
     */
    private final long interval;

    /**
     * MemArtsCC traffic report Thread pool & Schedule
     */
    private ScheduledExecutorService reportPool;

    private ScheduledFuture reportSchedule;

    public TrafficStatisticsReporter(TrafficStatistics trafficStatistics,
                                     MemArtsCCClient memArtsCCClient, long interval) {
        this.trafficStatistics = trafficStatistics;
        this.memArtsCCClient = memArtsCCClient;
        this.interval = interval;
    }

    public void startReport() {
        initThreadPool();
        initReportSchedule();
    }

    private void initThreadPool() {
        reportPool = new ScheduledThreadPoolExecutor(1,
                newDaemonThreadFactory("obs-traffic-statistics-report"));
    }

    private void initReportSchedule() {
        reportSchedule = reportPool.scheduleAtFixedRate(
                this::reportTraffic, interval, interval, TimeUnit.SECONDS);
    }

    private void reportTraffic() {
        if (memArtsCCClient == null || trafficStatistics == null) {
            if (memArtsCCClient == null) {
                LOG.debug("memArtsCCClient is null, statistics cannot be reported.");
            }
            if (trafficStatistics == null) {
                LOG.debug("trafficStatistics is null, statistics cannot be reported.");
            }
            return;
        }

        memArtsCCClient.reportReadStatistics(trafficStatistics);
        LOG.debug("Statistics has been reported: Q:{} Q`:{} Q2:{} Q1:{}",
                trafficStatistics.getStatistics(TrafficStatistics.TrafficType.Q),
                trafficStatistics.getStatistics(TrafficStatistics.TrafficType.QDot),
                trafficStatistics.getStatistics(TrafficStatistics.TrafficType.Q2),
                trafficStatistics.getStatistics(TrafficStatistics.TrafficType.Q1));

        trafficStatistics.clearStatistics();
    }

    public void shutdownReport() {
        if (memArtsCCClient == null || trafficStatistics == null) {
            return;
        }
        reportTraffic();
        // cancel report schedule
        try {
            reportSchedule.cancel(true);
            if (reportSchedule.isCancelled()) {
                LOG.debug("TrafficStatistics reportSchedule is canceled.");
            }
            reportPool.shutdownNow();
            if (reportPool.isShutdown()) {
                LOG.debug("TrafficStatistics reportPool is shutdowned.");
            }
        } catch (Exception e) {
            LOG.debug("Exception occurred when canceling scheduledFuture");
        }
    }
}

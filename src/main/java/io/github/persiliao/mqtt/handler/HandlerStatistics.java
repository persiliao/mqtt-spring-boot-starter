package io.github.persiliao.mqtt.handler;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe processing counters for a single handler bean.
 *
 * @since 3.0.0
 * @author Persi.Liao <xiangchu.liao@gmail.com>
 */
final class HandlerStatistics {

    private final AtomicLong total = new AtomicLong();
    private final AtomicLong success = new AtomicLong();
    private final AtomicLong failure = new AtomicLong();
    private final AtomicLong totalNanos = new AtomicLong();
    private final AtomicLong maxNanos = new AtomicLong();
    private final AtomicLong minNanos = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong lastProcessedAt = new AtomicLong();

    /**
     * Records a successful processing.
     *
     * @param elapsedNanos the processing time in nanoseconds
     */
    void recordSuccess(long elapsedNanos) {
        total.incrementAndGet();
        success.incrementAndGet();
        totalNanos.addAndGet(elapsedNanos);
        maxNanos.accumulateAndGet(elapsedNanos, Math::max);
        minNanos.accumulateAndGet(elapsedNanos, Math::min);
        lastProcessedAt.set(System.currentTimeMillis());
    }

    /**
     * Records a failed processing.
     */
    void recordFailure() {
        total.incrementAndGet();
        failure.incrementAndGet();
        lastProcessedAt.set(System.currentTimeMillis());
    }

    /**
     * @return a snapshot of the counters suitable for logging or exposure
     */
    Map<String, Object> toMap() {
        long totalMessages = total.get();
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("total", totalMessages);
        stats.put("success", success.get());
        stats.put("failure", failure.get());
        stats.put("successRate", totalMessages == 0 ? 0.0 : success.get() * 100.0 / totalMessages);
        stats.put("averageMs", totalMessages == 0 ? 0.0 : totalNanos.get() / (double) totalMessages / 1_000_000.0);
        stats.put("maxMs", maxNanos.get() / 1_000_000.0);
        stats.put("minMs", minNanos.get() == Long.MAX_VALUE ? 0.0 : minNanos.get() / 1_000_000.0);
        stats.put("lastProcessedAt", lastProcessedAt.get());
        return stats;
    }
}

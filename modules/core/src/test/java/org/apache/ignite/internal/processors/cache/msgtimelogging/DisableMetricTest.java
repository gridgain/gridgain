package org.apache.ignite.internal.processors.cache.msgtimelogging;

import javax.management.MalformedObjectNameException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.metric.HistogramMetric;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_MESSAGES_TIME_LOGGING;

/**
 *
 */
public class DisableMetricTest extends GridCacheMessagesTimeLoggingAbstractTest {
    /** {@inheritDoc} */
    @Override void setEnabledParam() {
        System.setProperty(IGNITE_ENABLE_MESSAGES_TIME_LOGGING, "not boolean value");
    }

    /**
     * Tests metrics disabling
     */
    @Test
    public void testDisabledMetric() throws MalformedObjectNameException {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        HistogramMetric metric = getMetric(0, grid(1), GridDhtTxPrepareRequest.class, true);

        assertNull("Metrics unexpectedly enabled", metric);
    }
}

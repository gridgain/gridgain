/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.msgtimelogging;

import javax.management.MalformedObjectNameException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
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

        populateCache(cache);

        HistogramMetric metric = getMetric(0, 1, GridDhtTxPrepareResponse.class);

        assertNull("Metrics unexpectedly enabled", metric);
    }
}

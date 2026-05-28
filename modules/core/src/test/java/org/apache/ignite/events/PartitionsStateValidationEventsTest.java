/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.events;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Tests for triggering PartitionsValidationEvent in case validation failed or validation succeeded.
 */
public class PartitionsStateValidationEventsTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final List<PartitionsStateValidationEvent> evts = new CopyOnWriteArrayList<>();

    /** */
    private static CountDownLatch latch;

    /** */
    private IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
                .setClientConnectorConfiguration(new ClientConnectorConfiguration());

        cfg.setCacheConfiguration(new CacheConfiguration(CACHE_NAME).setBackups(1));

        cfg.setIncludeEventTypes(
                EventType.EVTS_PARTITIONS_STATE_VALIDATION
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        evts.clear();

        stopAllGrids();

        ignite = startGrids(2);

        ignite.events()
                .localListen(
                        new IgnitePredicate<PartitionsStateValidationEvent>() {
                            @Override public boolean apply(PartitionsStateValidationEvent evt) {
                                evts.add(evt);

                                latch.countDown();

                                return true;
                            }
                        }, EventType.EVTS_PARTITIONS_STATE_VALIDATION
                );

        // There are exactly five events of this type should be triggered, but only three are handled:
        // - the first node startup (the first event (EVT_PARTITIONS_STATE_VALIDATION_SUCCEEDED) triggered)
        // - the second node started (the second event (EVT_PARTITIONS_STATE_VALIDATION_SUCCEEDED) triggered)
        // - setting up a local event listener
        // - the cache "cache" rebalanced to the second node (the third event (EVT_PARTITIONS_STATE_VALIDATION_SUCCEEDED) triggered)
        // - the third node started (the fourth event (EVT_PARTITIONS_STATE_VALIDATION_FAILED, if there is inconsistency,
        // otherwise EVT_PARTITIONS_STATE_VALIDATION_SUCCEEDED) triggered)
        // - the cache "cache" rebalanced to the third node (the fifth event (EVT_PARTITIONS_STATE_VALIDATION_SUCCEEDED)
        // triggered as the inconsistencies have been fixed by rebalance)
        latch = new CountDownLatch(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Tests that EVT_PARTITIONS_STATE_VALIDATION_FAILED is triggered, in case, validation happened and event contains
     * partitions which failed validation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRecordingPartitionsValidationEventWhenValidationFailed() throws Exception {
        for (int i = 0; i < 500; i++)
            ignite.cache(CACHE_NAME).put(i, i);

        for (GridDhtLocalPartition partition : ignite.cachex(CACHE_NAME).context().topology().localPartitions()) {
            if (partition.id() % 2 == 0) {
                partition.updateCounter(100500L);
            }
        }

        // Trigger exchange.
        startGrid(2);

        awaitPartitionMapExchange();

        assertTrue("Failed to wait for partition validation." + latch.getCount(), latch.await(15, SECONDS));

        assertEquals(3, evts.size());

        PartitionsStateValidationEvent evt = evts.get(0);

        assertNotNull(evt);

        assertEquals(EventType.EVT_PARTITIONS_STATE_VALIDATION_SUCCEEDED, evt.type());
        assertEquals("Partitions state validation succeeded.", evt.message());
        assertTrue(evt.parts().isEmpty());
        assertEquals(2, evt.topVer().topologyVersion());

        evt = evts.get(1);

        assertNotNull(evt);

        assertEquals(EventType.EVT_PARTITIONS_STATE_VALIDATION_FAILED, evt.type());
        assertEquals("Partitions state validation failed.", evt.message());
        assertEquals(250, evt.parts().get(CACHE_NAME).size());
        assertEquals(3, evt.topVer().topologyVersion());

        evt = evts.get(2);

        assertNotNull(evt);

        assertEquals(EventType.EVT_PARTITIONS_STATE_VALIDATION_SUCCEEDED, evt.type());
        assertEquals("Partitions state validation succeeded.", evt.message());
        assertTrue(evt.parts().isEmpty());
        assertEquals(3, evt.topVer().topologyVersion());
    }

    /**
     * Tests that EVT_PARTITIONS_STATE_VALIDATION_FAILED is not triggered, in case, validation hasn't found
     * any inconsistencies.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRecordingPartitionsValidationEventWhenValidationPassed() throws Exception {
        for (int i = 0; i < 500; i++)
            ignite.cache(CACHE_NAME).put(i, i);

        // Trigger exchange.
        startGrid(2);

        awaitPartitionMapExchange();

        assertTrue("Failed to wait for partition validation." + latch.getCount(), latch.await(5, SECONDS));

        assertEquals(3, evts.size());

        for (PartitionsStateValidationEvent evt : evts) {
            assertEquals("Partitions state validation succeeded.", evt.message());
            assertEquals(EventType.EVT_PARTITIONS_STATE_VALIDATION_SUCCEEDED, evt.type());
        }
    }
}

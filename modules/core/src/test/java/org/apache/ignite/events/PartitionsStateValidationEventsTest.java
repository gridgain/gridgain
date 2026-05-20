/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 * Tests for triggering PartitionsValidationEvent in case validation failed.
 */
public class PartitionsStateValidationEventsTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final Collection<Event> evts = new ArrayList<>();

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
                EventType.EVT_PARTITIONS_STATE_VALIDATION_FAILED
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
                        new IgnitePredicate<Event>() {
                            @Override public boolean apply(Event evt) {
                                evts.add(evt);

                                latch.countDown();

                                return true;
                            }
                        }, EventType.EVT_PARTITIONS_STATE_VALIDATION_FAILED
                );

        latch = new CountDownLatch(1);

        ignite.cluster().state(ACTIVE);
        ignite.cluster().baselineAutoAdjustEnabled(false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Tests that event is triggered in case of found inconsistencies and all required information is delivered in event.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRecordingPartitionsValidationEvent() throws Exception {
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

        assertTrue("Failed to wait for partition validation.", latch.await(5, SECONDS));

        assertEquals(1, evts.size());

        Iterator<Event> it = evts.iterator();

        assertTrue(it.hasNext());

        Event evt = it.next();

        assertNotNull(evt);

        assertEquals(EventType.EVT_PARTITIONS_STATE_VALIDATION_FAILED, evt.type());
        assertEquals(250, ((PartitionsStateValidationEvent)evt).parts().get(CACHE_NAME).size());
        System.out.println(((PartitionsStateValidationEvent)evt).topVer());
        assertEquals(3, ((PartitionsStateValidationEvent)evt).topVer().topologyVersion());
    }

    /**
     * Tests that there is no event in case the validation passed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionsValidationEventNotRecorded() throws Exception {
        for (int i = 0; i < 500; i++)
            ignite.cache(CACHE_NAME).put(i, i);

        // Trigger exchange.
        startGrid(2);

        awaitPartitionMapExchange();

        assertFalse("The 'Validation Failed' event was sent in error.", latch.await(5, SECONDS));

        assertEquals(0, evts.size());
    }
}

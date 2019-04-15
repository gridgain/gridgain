/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */
package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that starvation in common pool won't prevent checkpoint.
 */
public class CommonPoolStarvationCheckpointTest extends GridCommonAbstractTest {
    /** Entries count. */
    private static final int ENTRIES_COUNT = (1 << 13) + 100;

    /** Cache name. */
    protected static final String CACHE_NAME = "cache";

    /** Page size. */
    public static final int PAGE_SIZE = 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(200L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setCheckpointFrequency(Long.MAX_VALUE / 2)
            .setPageSize(PAGE_SIZE)
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.CHECKPOINT_PARALLEL_SORT_THRESHOLD, value = "0")
    public void testCommonPoolStarvation() throws Exception {
        IgniteEx grid = startGrid(0);

        grid.cluster().active(true);

        IgniteCache<Integer, OnePageValue> cache = grid.cache(CACHE_NAME);

        forceCheckpoint();

        for (int i = 0; i < ENTRIES_COUNT; i++)
            cache.put(i, new OnePageValue());

        CountDownLatch latch = new CountDownLatch(1);

        ForkJoinPool commonPool = ForkJoinPool.commonPool();

        for (int i = 0; i < Runtime.getRuntime().availableProcessors() * 10; i++) {
            commonPool.submit(() -> {
                try {
                    latch.await();
                }
                catch (InterruptedException e) {
                    throw new IgniteInterruptedException(e);
                }
            });
        }

        forceCheckpoint();

        latch.countDown();
    }

    /**
     * Test value that will be stored in exactly one data page.
     */
    private static class OnePageValue {
        /** Payload. */
        private final byte[] payload;

        /**
         * Default constructor.
         */
        public OnePageValue() {
            payload = new byte[PAGE_SIZE * 2 / 3];

            payload[ThreadLocalRandom.current().nextInt(payload.length)] = 1;
        }
    }
}

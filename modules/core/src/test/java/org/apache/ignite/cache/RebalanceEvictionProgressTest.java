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

package org.apache.ignite.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;


@SuppressWarnings("resource")
public class RebalanceEvictionProgressTest extends GridCommonAbstractTest {

    private static final int PARTS_CNT = 1024;
    private static final int BACKUPS = 1;

    private static final String TS_CLEANUP_STR = "5000";
    private static final int TS_CLEANUP = Integer.parseInt(TS_CLEANUP_STR) + 1000;

    private final ListeningTestLogger logTracer = new ListeningTestLogger(log);

    @Override
    public String getTestIgniteInstanceName(int idx) {
        return "n" + idx;
    }

    /** {@inheritDoc} */
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
                .setConsistentId(igniteInstanceName)
                .setDataStorageConfiguration(new DataStorageConfiguration()
                        .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                                .setPersistenceEnabled(true)))
                .setCacheConfiguration(
                        new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                                .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT))
                                .setBackups(BACKUPS));

        cfg.setGridLogger(logTracer);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override
    protected void beforeTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override
    protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /** Fail */
    @Test
    @WithSystemProperty(key = "IGNITE_PDS_FORCED_CHECKPOINT_ON_NODE_STOP", value = "true")
    @WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = TS_CLEANUP_STR)
    public void evictionOnRebalanceTest2() throws Exception {
        execute((cache, writer) -> {
            for (int p = 0; p < PARTS_CNT; p++) {
                writer.deleteFrom(p, 1);
            }
            U.sleep(TS_CLEANUP);

            LogListener lsnr = listener("Start clearAll() for part=0");

            stopGrid(0);
            for (int p = 0; p < PARTS_CNT; p++) {
                writer.deleteFrom(p, 1);
            }
            startGrid(0);

            U.sleep(1000);
            System.out.println("\n\n\n>> End");

            assertFalse("Partition was evicted (cleared)", lsnr.check());
        });
    }

    /**
     * Set up 2 nodes cluster + client + prefill cache
     */
    private void execute(Execution execution) throws Exception {
        startGrid(0);
        startGrid(1);

        IgniteEx client = startClientGrid("client");

        client.cluster().state(ClusterState.ACTIVE);
        client.cluster().disableWal(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        IgniteCache<Integer, String> cache = client.cache(DEFAULT_CACHE_NAME);
        PartitionWriter writer = new PartitionWriter(cache, client.affinity(DEFAULT_CACHE_NAME));

        for (int i = 0; i < 100_000; i++) {
            cache.put(i, "val-" + i);
        }

        execution.execute(cache, writer);
    }

    private LogListener listener(String target) {
        LogListener lsnr = LogListener
                .matches((s) -> s.contains(target))
                .build();

        logTracer.registerListener(lsnr);
        return lsnr;
    }

    private static class PartitionWriter {
        private final IgniteCache<Integer, String> cache;
        private final Affinity<Integer> affinity;

        public PartitionWriter(IgniteCache<Integer, String> cache, Affinity<Integer> affinity) {
            this.cache = cache;
            this.affinity = affinity;
        }

        public void deleteFrom(int part, int cnt) {
            int size = cache.size(CachePeekMode.PRIMARY);
            int removed = 0;

            for (int i = 0; i < size; i++) {
                if (affinity.partition(i) == part && removed < cnt && cache.get(i) != null) {
                    cache.remove(i);
                    removed++;
                }
            }

            System.out.println(">> Removed " + removed + " entries from part=" + part);
        }

        public void writeTo(int part, int cnt) {
            int size = cache.size(CachePeekMode.PRIMARY);
            int added = 0;

            for (int i = 0; i < 1_000_000; i++) {
                if (added < cnt && affinity.partition(i) == part) {
                    cache.put(i, "val-" + i);
                    added++;
                }
            }

            System.out.println(">> Written " + added + " entries to part=" + part);
        }

        public void appendTo(int part, int cnt) {
            int size = cache.size(CachePeekMode.PRIMARY);
            int added = 0;


            for (int i = size; i < 1_000_000; i++) {
                if (added < cnt && affinity.partition(i) == part) {
                    cache.put(i, "val-" + i);
                    added++;
                }
            }

            System.out.println(">> Appended " + added + " entries to part=" + part);
        }
    }

    private interface Execution {
        void execute(IgniteCache<Integer, String> cache,
                     PartitionWriter writer
        ) throws Exception;
    }

}

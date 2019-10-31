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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 * Test partitions consistency in various scenarios when all rebalance is historical.
 */
@WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
public class TxPartitionCounterStateConsistencyHistoryRebalanceTest extends TxPartitionCounterStateConsistencyTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(100000000L);
        cfg.setClientFailureDetectionTimeout(100000000L);

        return cfg;
    }

    /** */
    @Test
    public void testPutRemoveReorder() throws Exception {
        backups = 1;

        Ignite crd = startGrids(2);
        crd.cluster().active(true);

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        final int part = 0;

        List<Integer> keys = partitionKeys(crd.cache(DEFAULT_CACHE_NAME), part, 2, 0);

        for (int p = 0; p < PARTS_CNT; p++)
            cache.put(p, 0);

        forceCheckpoint();

        String name = grid(1).name();

        stopGrid(1);

        awaitPartitionMapExchange();

        cache.put(keys.get(1), 1);
        cache.remove(keys.get(1));

        TestRecordingCommunicationSpi.spi(crd).blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionSupplyMessage) {
                GridDhtPartitionSupplyMessage sup = (GridDhtPartitionSupplyMessage)msg;

                if (sup.groupId() != CU.cacheId(DEFAULT_CACHE_NAME))
                    return false;

                Map<Integer, CacheEntryInfoCollection> infos = U.field(sup, "infos");

                CacheEntryInfoCollection col = infos.get(part);

                List<GridCacheEntryInfo> infos0 = col.infos();

                // Reorder remove and put.
                GridCacheEntryInfo e1 = infos0.get(0);
                infos0.set(0, infos0.get(1));
                infos0.set(1, e1);
            }

            return false;
        });

        startGrid(1);

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

//        stopGrid(true, name);
//
//        IgniteEx tmp = startGrid(1);
//
//        GridDhtLocalPartition part0 = tmp.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(part);
//
//        assertTrue(part0.dataStore().tombstonesCount() > 0);
    }
}

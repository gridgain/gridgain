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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 * Test partitions consistency in various scenarios.
 */
@WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
public class TxPartitionCounterStateConsistencyHistoryRebalanceTest extends TxPartitionCounterStateConsistencyTest {
    /**
     * TODO
     */
    @Test
    public void testPartitionConsistencyDuringRebalance_SupplyMessageReorder() throws Exception {
        backups = 2;

        Ignite prim = startGridsMultiThreaded(SERVER_NODES);

        int[] primaryParts = prim.affinity(DEFAULT_CACHE_NAME).primaryPartitions(prim.cluster().localNode());

        int finalPart = primaryParts[0];

        List<Integer> keys = partitionKeys(prim.cache(DEFAULT_CACHE_NAME), finalPart, 2, 0);

        prim.cache(DEFAULT_CACHE_NAME).put(keys.get(0), keys.get(0));

        forceCheckpoint();

        List<Ignite> backups = backupNodes(keys.get(0), DEFAULT_CACHE_NAME);

        assertFalse(backups.contains(prim));

        stopGrid(true, backups.get(0).name());

        prim.cache(DEFAULT_CACHE_NAME).put(keys.get(1), keys.get(1));
        prim.cache(DEFAULT_CACHE_NAME).remove(keys.get(1));

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(prim);

        spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message message) {
                if (message instanceof GridDhtPartitionSupplyMessage) {
                    GridDhtPartitionSupplyMessage sm = (GridDhtPartitionSupplyMessage)message;

                    Map<Integer, CacheEntryInfoCollection> map = U.field(sm, "infos");

                    CacheEntryInfoCollection col = map.get(finalPart);

                    List<GridCacheEntryInfo> infos = col.infos();

                    GridCacheEntryInfo tmp = infos.get(0);
                    infos.set(0, infos.get(1));
                    infos.set(1, tmp);

                    System.out.println();
                }

                return false;
            }
        });

        startGrid(backups.get(0).name());

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(prim, DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);
    }
}

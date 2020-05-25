/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

@WithSystemProperty(key = IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
public class CicledRebalanceTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setAffinity(new RendezvousAffinityFunction(false, 16))
                .setBackups(1));
    }

    @Test
    public void test() throws Exception {
        IgniteEx ignite0 = startGrids(2);

        ignite0.cluster().active(true);

        loadData();

        awaitPartitionMapExchange();
        forceCheckpoint();

        AtomicBoolean hasFullRebalance = new AtomicBoolean();

        for (int i = 0; i < 5; i++) {
            info("Iter: " + i);

            stopGrid(1);

            loadData();

            awaitPartitionMapExchange();
//            forceCheckpoint();

            startNodeAndRecordDemandMsg(hasFullRebalance, 1);

            awaitPartitionMapExchange();
//            forceCheckpoint();

            assertFalse("Assert on iter " + i, hasFullRebalance.get());

            stopGrid(0);

            loadData();

            awaitPartitionMapExchange();
//            forceCheckpoint();

            startNodeAndRecordDemandMsg(hasFullRebalance, 0);

            awaitPartitionMapExchange();
//            forceCheckpoint();

            assertFalse(hasFullRebalance.get());
        }
    }

    /**
     * @param hasFullRebalance Has full rebalance flag.
     * @throws Exception If failed.
     */
    @NotNull private void startNodeAndRecordDemandMsg(AtomicBoolean hasFullRebalance, int nodeNum) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(nodeNum));

        TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

        spi.record((node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage)msg;

                hasFullRebalance.compareAndSet(false, !F.isEmpty(demandMsg.partitions().fullSet()));

                return true;
            }

            return false;
        });

        startGrid(cfg);
    }

    public void loadData() {
        Random random = new Random();

        Ignite ignite = G.allGrids().get(0);

        try (IgniteDataStreamer streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < 100; i++) {
                Integer ranDomKey = random.nextInt(10_000);

                streamer.addData(ranDomKey, "Val " + ranDomKey);
            }
        }
    }
}

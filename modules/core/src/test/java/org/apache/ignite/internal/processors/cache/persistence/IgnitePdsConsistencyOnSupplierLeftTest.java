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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.Callable;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class IgnitePdsConsistencyOnSupplierLeftTest extends GridCommonAbstractTest {
    /** Parts. */
    public static final int PARTS = 128;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureDetectionTimeout(100000L);
        cfg.setClientFailureDetectionTimeout(100000L);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setConsistentId(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(50L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setWalSegmentSize(4 * 1024 * 1024)
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(8000);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS));
        ccfg.setBackups(2);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    public void testSupplierLeft() throws Exception {
        IgniteEx crd = startGrids(4); // TODO multithreaded.
        crd.cluster().active(true);

        for (int i = 0; i < PARTS; i++)
            crd.cache(DEFAULT_CACHE_NAME).put(i, i);

        forceCheckpoint();

        stopGrid(1);

        for (int i = 0; i < PARTS; i++)
            crd.cache(DEFAULT_CACHE_NAME).put(i, i + 1);

        TestRecordingCommunicationSpi spi0 = TestRecordingCommunicationSpi.spi(grid(0));
        TestRecordingCommunicationSpi spi2 = TestRecordingCommunicationSpi.spi(grid(2));
        TestRecordingCommunicationSpi spi3 = TestRecordingCommunicationSpi.spi(grid(3));

        IgniteBiPredicate<ClusterNode, Message> pred = new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                return msg instanceof GridDhtPartitionSupplyMessage;
            }
        };

        spi0.blockMessages(pred);
        spi2.blockMessages(pred);
        spi3.blockMessages(pred);

        GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(1);

                return null;
            }
        });

        spi0.waitForBlocked();
        spi2.waitForBlocked();
        spi3.waitForBlocked();

        System.out.println();

//        spi0.stopBlock();
//        spi2.stopBlock();
        spi3.stopBlock();

        doSleep(5000);

        awaitPartitionMapExchange();
    }
}

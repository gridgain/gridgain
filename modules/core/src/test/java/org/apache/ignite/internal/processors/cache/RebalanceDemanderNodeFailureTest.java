/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplier;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Test;

import java.util.HashMap;
import java.util.Set;
import java.util.UUID;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

public class RebalanceDemanderNodeFailureTest extends GridCommonAbstractTest {

    /** Cache name. */
    private static final String CACHE_NAME = "cache" + UUID.randomUUID().toString();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        cfg.setShutdownPolicy(ShutdownPolicy.IMMEDIATE);

        cfg.setRebalanceBatchSize(100);
        cfg.setRebalanceBatchesPrefetchCount(1);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true))
                .setCheckpointFrequency(4_000)
                .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration<Integer, String> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName(CACHE_NAME);
        cacheConfiguration.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheConfiguration.setBackups(1);

        cfg.setCacheConfiguration(cacheConfiguration);

        return cfg;
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    @Test
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
    public void haltNodeDuringHistoricalRebalance() throws Exception {
        haltDemanderNodeDuringRebalance(false);
    }

    @Test
    public void haltNodeDuringFullRebalance() throws Exception {
        haltDemanderNodeDuringRebalance(true);
    }

    private void haltDemanderNodeDuringRebalance(boolean fullRebalance) throws Exception {
        IgniteEx srv0 = startGrids(2);

        srv0.cluster().state(ClusterState.ACTIVE);

        for (int i = 0; i < 10_000; i++) {
            srv0.cache(CACHE_NAME).put(i,UUID.randomUUID().toString());
        }

        stopGrid(1);

        for (int i = 10_001; i < 12_000; i++) {
            srv0.cache(CACHE_NAME).put(i,UUID.randomUUID().toString());
        }

        forceCheckpoint(srv0);

        TestRecordingCommunicationSpi spi0 = TestRecordingCommunicationSpi.spi(srv0);
        spi0.blockMessages(TestRecordingCommunicationSpi.blockSupplyMessageForGroup(CU.cacheId(CACHE_NAME)));

        IgniteEx srv1 = startGrid(1);

        spi0.waitForBlocked();

        checkRebalanceType(fullRebalance, srv0);

        ((IgniteProcessProxy)srv1).kill();

        spi0.stopBlock();

        checkTopology(1);

        startGrid(1);

        awaitPartitionMapExchange();
    }

    private void checkRebalanceType(boolean fullRebalance, IgniteEx ignite) throws IgniteInterruptedCheckedException {
        GridCachePreloader preloader = ignite.cachex(CACHE_NAME).context().group().preloader();

        GridDhtPartitionSupplier supplier = U.field(preloader, "supplier");

        HashMap<Object, Object> map = U.field(supplier, "scMap");

        assertTrue(GridTestUtils.waitForCondition(() -> map.entrySet().iterator().hasNext(), 10_000));

        Object supplyContext = map.entrySet().iterator().next().getValue();

        IgniteRebalanceIterator iterator = U.field(supplyContext, "iterator");

        Set<Integer> parts = U.field(supplyContext, "remainingParts");

        if (fullRebalance) {
            assertTrue(parts.stream().noneMatch(iterator::historical));
        } else {
            assertTrue(parts.stream().allMatch(iterator::historical));
        }
    }
}

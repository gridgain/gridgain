/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntry;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistory;
import org.apache.ignite.internal.processors.cluster.IgniteClusterMXBeanImpl;
import org.apache.ignite.mxbean.IgniteClusterMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@GridCommonTest(group = "Kernal Self")

/**
 * Cluster wide checkpointing test.
 */
@RunWith(Parameterized.class)
public class GridCheckpointTest extends GridCommonAbstractTest {

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

    @Parameterized.Parameters(name = "{0}")
    public static List<Object> testData() {
        return Arrays.asList(new Object[] {1,3});
    }

    /** */
    @Parameterized.Parameter(0)
    public int gridSize;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        CacheConfiguration<Object, Object> ccfg1 = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, 16));

        cfg.setCacheConfiguration(ccfg1);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(1024 * 1024)
            .setCheckpointFrequency(Integer.MAX_VALUE)
            .setWalCompactionEnabled(true)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(100 * 1024 * 1024)
            );

        cfg.setDataStorageConfiguration(dbCfg);

        return cfg;
    }

    @Test
    public void testForceCheckpointing() throws Exception {
        IgniteEx g1 = startGrids(gridSize);

        for (int i = 0; i < gridSize; i++) {
            assertEquals("Number of checkpoints for node: " + i, 0, numOfCheckpoints(i));
            assertNull("Last checkpoint for node: " + i, lastCheckpoint(i));
        }

        g1.cluster().state(ClusterState.ACTIVE);

        g1.cluster().checkpoint();

        for (int i = 0; i < gridSize; i++) {
            assertEquals("Number of checkpoints for node: " + i, 1, numOfCheckpoints(i));
            assertNotNull("Last checkpoint for node: " + i, lastCheckpoint(i));
        }
    }

    @Test
    public void testForceCheckpointingJmx() throws Exception {
        IgniteEx g1 = startGrids(gridSize);

        for (int i = 0; i < gridSize; i++) {
            assertEquals("Number of checkpoints for node: " + i, 0, numOfCheckpoints(i));
            assertNull("Last checkpoint for node: " + i, lastCheckpoint(i));
        }

        g1.cluster().state(ClusterState.ACTIVE);

        IgniteClusterMXBean clustMxBean = getMxBean(
            getTestIgniteInstanceName(0),
            "IgniteCluster",
            IgniteClusterMXBean.class,
            IgniteClusterMXBeanImpl.class
        );
        clustMxBean.checkpoint();

        for (int i = 0; i < gridSize; i++) {
            assertEquals("Number of checkpoints for node: " + i, 1, numOfCheckpoints(i));
            assertNotNull("Last checkpoint for node: " + i, lastCheckpoint(i));
        }
    }

    private void loadData(int idx) throws Exception {
        Map<Integer, String> vals = new HashMap<>();

        for (int i = 1; i <= 10; i++)
            vals.put(i, Integer.toString(i));

        loadAll(jcache(idx), vals.keySet(), true);
    }

    int numOfCheckpoints(int idx) {
        return checkpointHistory(idx).checkpoints().size();
    }

    CheckpointEntry lastCheckpoint(int idx) {
        return checkpointHistory(idx).lastCheckpoint();
    }

    private CheckpointHistory checkpointHistory(int idx) {
        final GridCacheDatabaseSharedManager gridDb = (GridCacheDatabaseSharedManager)grid(idx).context().cache().context().database();
        final CheckpointHistory checkpointHist = gridDb.checkpointHistory();
        assert null != checkpointHist;

        return checkpointHist;
    }
}

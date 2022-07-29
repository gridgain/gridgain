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

import org.apache.ignite.IgniteCache;
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

@GridCommonTest(group = "Kernal Self")

/**
 * Cluster wide checkpointing test.
 */
public class GridCheckpointTest extends GridCommonAbstractTest {
    /** */
    public GridCheckpointTest() {
        super(/*start Grid*/false);
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

    /** */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = defaultCacheConfiguration();
        cc.setName(DEFAULT_CACHE_NAME);

        return cc;
    }

    @Test
    public void testForceCheckpointing() throws Exception {
        IgniteEx ignite1 = startGrid(1);

        final GridCacheDatabaseSharedManager gridDb = (GridCacheDatabaseSharedManager)ignite1.context().cache().context().database();
        final CheckpointHistory checkpointHist = gridDb.checkpointHistory();
        ignite1.configuration().getCheckpointSpi();
        assert null != checkpointHist;

        final int numOfCheckpointsBefore = checkpointHist.checkpoints().size();
        final CheckpointEntry lastCheckpointBefore = checkpointHist.lastCheckpoint();

        ignite1.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> jcache = jcache(1);
        jcache.put(1, 1);

        ignite1.cluster().checkpoint();

        final int numOfCheckpointsAfter = checkpointHist.checkpoints().size();
        final CheckpointEntry lastCheckpointAfter = checkpointHist.lastCheckpoint();

        assertTrue(numOfCheckpointsAfter > numOfCheckpointsBefore);
        assertNotSame(lastCheckpointBefore, lastCheckpointAfter);
    }

    @Test
    public void testForceCheckpointingJmx() throws Exception {
        IgniteEx ignite1 = startGrid(1);

        final GridCacheDatabaseSharedManager gridDb = (GridCacheDatabaseSharedManager)ignite1.context().cache().context().database();
        final CheckpointHistory checkpointHist = gridDb.checkpointHistory();
        ignite1.configuration().getCheckpointSpi();
        assert null != checkpointHist;

        final int numOfCheckpointsBefore = checkpointHist.checkpoints().size();
        final CheckpointEntry lastCheckpointBefore = checkpointHist.lastCheckpoint();

        ignite1.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> jcache = jcache(1);
        jcache.put(1, 1);

        IgniteClusterMXBean clustMxBean = getMxBean(
            getTestIgniteInstanceName(1),
            "IgniteCluster",
            IgniteClusterMXBean.class,
            IgniteClusterMXBeanImpl.class
        );
        clustMxBean.checkpoint();

        final int numOfCheckpointsAfter = checkpointHist.checkpoints().size();
        final CheckpointEntry lastCheckpointAfter = checkpointHist.lastCheckpoint();

        assertTrue(numOfCheckpointsAfter > numOfCheckpointsBefore);
        assertNotSame(lastCheckpointBefore, lastCheckpointAfter);
    }
}

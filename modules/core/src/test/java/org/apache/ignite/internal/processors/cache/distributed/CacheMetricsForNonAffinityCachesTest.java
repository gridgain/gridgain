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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.AttributeNodeFilter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 *
 */
@RunWith(Parameterized.class)
public class CacheMetricsForNonAffinityCachesTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS_CNT = 32;

    /** */
    private static final String START_CACHE_ATTR = "has_cache";

    /** Possible values: 1, 0 */
    @Parameterized.Parameter(value = 0)
    public int nonAffinityIdx;

    /** Possible values: true, false */
    @Parameterized.Parameter(value = 1)
    public boolean dfltRegionPersistence;

    /** */
    @Parameterized.Parameters(name = "{0} {1}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        for (boolean persistent : new boolean[] {true, false}) {
            params.add(new Object[] {0, persistent});
            params.add(new Object[] {1, persistent});
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        DataRegionConfiguration dfltRegCfg = new DataRegionConfiguration();
        dfltRegCfg.setName(DEFAULT_CACHE_NAME).setPersistenceEnabled(dfltRegionPersistence);

        dsCfg.setDefaultDataRegionConfiguration(dfltRegCfg);

        cfg.setDataStorageConfiguration(dsCfg);

        // Do not start cache on non-affinity node.
        CacheConfiguration ccfg = defaultCacheConfiguration().setNearConfiguration(null).
            setNodeFilter(new AttributeNodeFilter(START_CACHE_ATTR, Boolean.TRUE)).
            setBackups(0).
            setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

        cfg.setCacheConfiguration(ccfg);

        if (getTestIgniteInstanceIndex(igniteInstanceName) != nonAffinityIdx) {
            cfg.setUserAttributes(F.asMap(START_CACHE_ATTR, Boolean.TRUE));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Test
    public void testCacheMetricsDontRaiseNPE() throws Exception {
        final IgniteEx crd = startGrids(2);
        crd.cluster().state(ACTIVE);

        awaitPartitionMapExchange();

        // Without the fix from GG-38238  DataRegionMetrics#getTotalUsedPages provides NPE
        assertEquals(0, grid(nonAffinityIdx).context().cache().context().database().memoryMetrics(DEFAULT_CACHE_NAME).getTotalUsedPages());
    }
}

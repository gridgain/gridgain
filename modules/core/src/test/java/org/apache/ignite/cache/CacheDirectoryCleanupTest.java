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


package org.apache.ignite.cache;

import java.io.File;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING;

/** */
@WithSystemProperty(key = IGNITE_DISABLE_WAL_DURING_REBALANCING, value = "false")
public class CacheDirectoryCleanupTest extends GridCommonAbstractTest {

    public static final String NODE_ID = "my-super-node";

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setConsistentId(NODE_ID);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setMetricsEnabled(true)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMetricsEnabled(true)
            .setPersistenceEnabled(true).setMaxSize(64 * 1024 * 1024)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        startGrids(1);

        grid(0).cluster().state(ClusterState.ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    public void testCleanup() throws Exception {
        IgniteEx ignite1 = grid(0);

        ignite1.createCache(new CacheConfiguration(DEFAULT_CACHE_NAME));
        IgniteCache c = ignite1.cache(DEFAULT_CACHE_NAME);
        c.put(30000, 30000);

        c.destroy();

        File workDir = ((FilePageStoreManager)ignite1.context().cache().context().pageStore()).workDir();
        assertFalse(new File(workDir, "cache-" + DEFAULT_CACHE_NAME).exists());
    }

}

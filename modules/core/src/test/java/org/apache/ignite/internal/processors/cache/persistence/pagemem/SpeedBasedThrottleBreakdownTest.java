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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 * Tests for the cases when high load might break down speed-based throttling protection.
 *
 * @see PagesWriteSpeedBasedThrottle
 */
public class SpeedBasedThrottleBreakdownTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                        // tiny CP buffer is required to reproduce this problem easily
                        .setCheckpointPageBufferSize(3_000_000)
                        .setName("dfltDataRegion")
                        .setPersistenceEnabled(true))
                .setCheckpointFrequency(200)
                .setWriteThrottlingEnabled(true);

        cfg.setDataStorageConfiguration(dbCfg);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setName(CACHE_NAME);
        ccfg1.setIndexedTypes(String.class, String.class);

        cfg.setCacheConfiguration(ccfg1);

        cfg.setConsistentId(gridName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3L * 60 * 1000;
    }

    /***/
    @Test
    public void speedBasedThrottleShouldNotAllowCPBufferBreakdownWhenCPBufferIsSmall() throws Exception {
        IgniteEx ignite = startGrids(1);
        
        ignite.cluster().state(ACTIVE);
        IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

        for (int i = 0; i < 100_000; i++) {
            cache.put(key(i), ThreadLocalRandom.current().nextDouble());
        }
    }

    /***/
    private static String key(int index) {
        return "key" + index;
    }

    /**
     * @throws Exception If failed.
     */
    private void deleteWorkFiles() throws Exception {
        cleanPersistenceDir();
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "snapshot", false));
    }
}

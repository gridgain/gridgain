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
package org.apache.ignite.internal.cluster;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for cluster id and cluster name features of IgniteCluster.
 */
public class IgniteClusterIdNameTest extends GridCommonAbstractTest {
    /** */
    private boolean isPersistenceEnabled;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setInitialSize(128 * 1024 * 1024)
                    .setMaxSize(128 * 1024 * 1024)
                    .setPersistenceEnabled(isPersistenceEnabled)
            );

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
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

    /**
     *
     * @throws Exception
     */
    @Test
    public void testInMemoryClusterId() throws Exception {
        Ignite ig0 = startGrid(0);

        UUID id0 = ig0.cluster().id();

        assertNotNull(id0);

        Ignite ig1 = startGrid(1);

        UUID id1 = ig1.cluster().id();

        assertEquals(id0, id1);

        stopAllGrids();

        ig0 = startGrid(0);

        assertNotSame(id0, ig0.cluster().id());
    }

    @Test
    public void testPersistentClusterId() throws Exception {
        isPersistenceEnabled = true;

        IgniteEx ig0 = startGrid(0);

        ig0.cluster().active(true);

        UUID id0 = ig0.cluster().id();

        stopAllGrids();

        ig0 = startGrid(0);

        assertEquals(id0, ig0.cluster().id());

        awaitPartitionMapExchange();
    }

    @Test
    public void testInMemoryClusterTag() throws Exception {

    }

    public void testPersistentClusterTag() throws Exception {

    }
}

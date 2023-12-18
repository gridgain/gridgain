/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Test cache directory name validation.
 */
@RunWith(Parameterized.class)
public class CacheDirectoryNameTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter()
    public boolean checkGroup;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "checkGroup={0}")
    public static Collection<?> parameters() {
        Collection<Object[]> params = new ArrayList<>();

        params.add(new Object[] {true});
        params.add(new Object[] {false});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** */
    @Test
    public void testDynamicCacheDirectoryContainsInvalidFileNameChars() throws Exception {
        IgniteEx srv = startGrid();

        srv.cluster().state(ClusterState.ACTIVE);

        int num = 0;

        for (String name : illegalCacheNames()) {
            CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME + num++);

            if (checkGroup)
                cfg.setGroupName(name);
            else
                cfg.setName(name);

            Throwable e = GridTestUtils.assertThrowsWithCause(
                    () -> srv.createCache(cfg), IllegalArgumentException.class
            );

            assertTrue(
                    e.getMessage().contains("Cache or group name contains the character that are not allowed")
                            || e.getMessage().contains("Cache name must not")
            );
        }
    }

    /** */
    @Test
    public void testStaticCacheDirectoryContainsInvalidFileNameChars() throws Exception {
        IgniteEx srv = startGrid();

        srv.cluster().state(ClusterState.ACTIVE);

        int num = 0;

        for (String name : illegalCacheNames()) {
            CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME + num++);

            if (checkGroup)
                cfg.setGroupName(name);
            else
                cfg.setName(name);

            String clientNodeName = testNodeName(10);

            IgniteConfiguration clietnCfg = getConfiguration(clientNodeName);

            clietnCfg.setCacheConfiguration(cfg);

            Throwable e = GridTestUtils.assertThrowsWithCause(
                    () -> startClientGrid(optimize(clietnCfg)), IgniteCheckedException.class
            );

            assertTrue(
                    e.getMessage().contains("Cache or group name contains the character that are not allowed")
                    || e.getMessage().contains("Cache name must not")
            );
        }
    }

    /** */
    private static List<String> illegalCacheNames() {
        List<String> illegalNames = new ArrayList<>();

        illegalNames.add("abc\\");
        illegalNames.add("abc/");
        illegalNames.add("abc:");
        illegalNames.add("abc?");
        illegalNames.add("abc\"");
        illegalNames.add("abc<");
        illegalNames.add("abc>");
        illegalNames.add("abc|");
        illegalNames.add("abc ");
        illegalNames.add("ab*c");

        return illegalNames;
    }
}

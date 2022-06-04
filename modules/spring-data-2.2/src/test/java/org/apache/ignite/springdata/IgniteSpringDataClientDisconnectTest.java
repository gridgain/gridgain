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

package org.apache.ignite.springdata;

import java.util.HashMap;
import java.util.Optional;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.springdata.compoundkey.City;
import org.apache.ignite.springdata.compoundkey.CityKey;
import org.apache.ignite.springdata.compoundkey.CityRepository;
import org.apache.ignite.springdata.compoundkey.ConfiguredApplicationConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class IgniteSpringDataClientDisconnectTest extends GridCommonAbstractTest {
    /** Application context */
    protected static AnnotationConfigApplicationContext ctx;

    /** City repository */
    protected static CityRepository repo;

    /** Cache name */
    private static final String CACHE_NAME = "City";

    private static Ignite ignite;

    /**
     * Performs context initialization before tests.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        ConfiguredApplicationConfiguration.cfg = getConfiguration("client")
            .setClientMode(true);

        ctx = new AnnotationConfigApplicationContext();
        ctx.register(ConfiguredApplicationConfiguration.class);
        ctx.refresh();

        repo = ctx.getBean(CityRepository.class);
    }

    /**
     * Load data
     * */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        loadData();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
    }

    /**
     * Clear data
     * */
    @Override protected void afterTest() throws Exception {
        repo.deleteAll();

        assertEquals(0, repo.count());

        super.afterTest();
    }

    /**
     * Performs context destroy after tests.
     */
    @Override protected void afterTestsStopped() throws Exception {
        ctx.close();

        super.afterTestsStopped();
    }

    /** Test */
    @Test
    public void test() throws Exception {
        assertEquals(Optional.of(new City("name 1", "district 1", 1)),
            repo.findById(new CityKey(1, "city 1")));

        ignite.close();

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            repo.findById(new CityKey(1, "city 1"));
        });

        assertFalse(fut.isDone());

        ignite = startGrid(0);

        fut.get(10_000);

        assertTrue(fut.isDone());
    }

    public void loadData() {
        HashMap<CityKey, City> rows = new HashMap<>();

        for (int i=0; i < 10; i++) {
            rows.put(new CityKey(i, "city " + i), new City("name " + i, "district " + i, i));
        }

        repo.save(rows);

        assertEquals(10, repo.count());
    }

    /** */
    protected Ignite client() {
        return ctx.getBean(Ignite.class);
    }
}

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

package org.apache.ignite.internal.metric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/** */
@RunWith(Parameterized.class)
public class CacheMetricsAddRemoveTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_GETS = "CacheGets";

    /** */
    public static final String CACHE_PUTS = "CachePuts";

    /** */
    public static final String CACHE_SIZE = "CacheSize";

    /** Cache modes. */
    @Parameterized.Parameters(name = "cacheMode={0},nearEnabled={1}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
            new Object[] {CacheMode.PARTITIONED, false},
            new Object[] {CacheMode.PARTITIONED, true},
            new Object[] {CacheMode.REPLICATED, false},
            new Object[] {CacheMode.REPLICATED, true}
        );
    }

    /** . */
    @Parameterized.Parameter(0)
    public CacheMode mode;

    /** Use index. */
    @Parameterized.Parameter(1)
    public boolean nearEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDataRegionConfigurations(
                    new DataRegionConfiguration().setName("persisted").setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        startGridsMultiThreaded(2);

        IgniteConfiguration clientCfg = getConfiguration("client")
            .setClientMode(true);

        startGrid(clientCfg);
    }

    /** */
    @Test
    public void testCacheMetricsAddRemove() throws Exception {
        String cachePrefix = cacheMetricsRegistryName(DEFAULT_CACHE_NAME, false);

        checkMetricsEmpty(cachePrefix);

        createCache();

        checkMetricsNotEmpty(cachePrefix);

        destroyCache();

        checkMetricsEmpty(cachePrefix);
    }

    /** */
    @Test
    public void testCacheMetricsNotRemovedOnStop() throws Exception {
        String cachePrefix = cacheMetricsRegistryName("other-cache", false);

        checkMetricsEmpty(cachePrefix);

        createCache("persisted", "other-cache");

        grid("client").cache("other-cache").put(1L, 1L);

        checkMetricsNotEmpty(cachePrefix);

        List<String> sz = collectCacheSizes(cachePrefix);

        //Cache will be stopped during deactivation.
        grid("client").cluster().state(ClusterState.INACTIVE);

        checkMetricsNotEmpty(cachePrefix);

        grid("client").cluster().state(ClusterState.ACTIVE);

        assertEquals(1L, grid("client").cache("other-cache").get(1L));

        checkMetricsNotEmpty(cachePrefix);

        checkCacheSizeMetric(cachePrefix, sz);

        destroyCache();

        checkMetricsEmpty(cachePrefix);
    }

    /** */
    private void destroyCache() throws InterruptedException {
        IgniteEx client = grid("client");

        for (String name : client.cacheNames())
            client.destroyCache(name);

        awaitPartitionMapExchange();
    }

    /** */
    private void createCache() throws InterruptedException {
        createCache(null, null);
    }

    /** */
    private void createCache(@Nullable String dataRegionName, @Nullable String cacheName) throws InterruptedException {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        if (dataRegionName != null)
            ccfg.setDataRegionName(dataRegionName);

        if (cacheName != null)
            ccfg.setName(cacheName);

        if (nearEnabled)
            ccfg.setNearConfiguration(new NearCacheConfiguration());

        grid("client").createCache(ccfg);

        awaitPartitionMapExchange();
    }

    /** */
    private void checkMetricsNotEmpty(String cachePrefix) {
        for (int i = 0; i < 2; i++) {
            GridMetricManager mmgr = metricManager(i);

            MetricRegistry mreg = mmgr.registry(cachePrefix);

            assertNotNull(mreg.findMetric(CACHE_GETS));
            assertNotNull(mreg.findMetric(CACHE_PUTS));

            if (nearEnabled) {
                mreg = mmgr.registry(metricName(cachePrefix, "near"));

                assertNotNull(mreg.findMetric(CACHE_GETS));
                assertNotNull(mreg.findMetric(CACHE_PUTS));
            }
        }
    }

    /** */
    private void checkMetricsEmpty(String cachePrefix) {
        for (int i = 0; i < 3; i++) {
            GridMetricManager mmgr = metricManager(i);

            MetricRegistry mreg = mmgr.registry(cachePrefix);

            assertNull(mreg.findMetric(metricName(cachePrefix, CACHE_GETS)));
            assertNull(mreg.findMetric(metricName(cachePrefix, CACHE_PUTS)));

            if (nearEnabled) {
                mreg = mmgr.registry(metricName(cachePrefix, "near"));

                assertNull(mreg.findMetric(CACHE_GETS));
                assertNull(mreg.findMetric(CACHE_PUTS));
            }
        }
    }

    /** */
    private List<String> collectCacheSizes(String cachePrefix) {
        List<String> res = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            GridMetricManager mmgr = metricManager(i);

            MetricRegistry mreg = mmgr.registry(cachePrefix);

            res.add(mreg.findMetric(CACHE_SIZE).getAsString());

            if (nearEnabled) {
                mreg = mmgr.registry(metricName(cachePrefix, "near"));

                res.add(mreg.findMetric(CACHE_SIZE).getAsString());
            }
        }

        return res;
    }

    /** */
    private void checkCacheSizeMetric(String cachePrefix, List<String> exp) {
        List<String> actual = collectCacheSizes(cachePrefix);

        assertEqualsCollections(exp, actual);
    }

    /** */
    private GridMetricManager metricManager(int gridIdx) {
        if (gridIdx < 2)
            return grid(0).context().metric();
        else
            return grid("client").context().metric();
    }
}

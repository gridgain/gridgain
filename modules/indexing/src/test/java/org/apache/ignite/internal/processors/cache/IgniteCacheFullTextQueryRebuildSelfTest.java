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

import java.util.HashSet;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests text index rebuild.
 */
public class IgniteCacheFullTextQueryRebuildSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            )
        );

        CacheConfiguration cache = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);

        cache.setQueryEntities(asList(
            new QueryEntity()
                .setKeyType(AffinityKey.class.getName())
                .setValueType(IndexedEntity.class.getName())
                .setKeyFields(new HashSet<>(asList("key", "affKey")))
                .addQueryField("key", Integer.class.getName(), null)
                .addQueryField("affKey", Integer.class.getName(), null)
                .addQueryField("val", String.class.getName(), null)
                .setIndexes(singleton(
                    new QueryIndex("val", QueryIndexType.FULLTEXT))
                ),
            new QueryEntity()
                .setKeyType(Long.class.getName())
                .setValueType(Integer.class.getName())
        ));

        cfg.setCacheConfiguration(cache);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFullTextQueryNodeRestart() throws Exception {
        Ignite crd = startGrids(1);

        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<AffinityKey, IndexedEntity> cache = crd.cache(DEFAULT_CACHE_NAME);

        assertEquals(0, cache.size());

        // Try to execute on empty cache first.
        QueryCursor<Cache.Entry<AffinityKey, IndexedEntity>> qry =
            cache.query(new TextQuery<AffinityKey, IndexedEntity>(IndexedEntity.class, "full"));

        assertThat(qry.getAll(), hasSize(0));

        // Now put indexed values into cache.
        for (int i = 0; i < 1000; i++) {
            IndexedEntity entity = new IndexedEntity("test full text " + i);

            grid(0).cache(DEFAULT_CACHE_NAME).put(new AffinityKey<>(i, i), entity);
        }

        qry = cache.query(new TextQuery<AffinityKey, IndexedEntity>(IndexedEntity.class, "full"));

        assertThat(qry.getAll(), hasSize(1000));

        forceCheckpoint();

        stopAllGrids();

        crd = startGrids(1);

        cache = jcache(0, DEFAULT_CACHE_NAME);

        qry = cache.query(new TextQuery<AffinityKey, IndexedEntity>(IndexedEntity.class, "full"));

        assertThat(qry.getAll(), hasSize(1000));
    }

    /** */
    private static class IndexedEntity {
        /** */
        private String val;

        /**
         * @param val Value.
         */
        private IndexedEntity(String val) {
            this.val = val;
        }
    }
}

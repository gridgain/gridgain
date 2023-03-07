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

package org.apache.ignite.internal.processors.query.h2;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_ALLOW_IMPLICIT_PK;

/**
 * Check the ability of creating tables without specifying a primary key.
 */
public class CreateTableImplicitPkTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setBackups(1))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(10 * 1024 * 1024)
                )
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        List<String> cacheNames =
            grid(0).cacheNames().stream().filter(name -> !name.equals(DEFAULT_CACHE_NAME)).collect(Collectors.toList());

        if (!F.isEmpty(cacheNames)) {
            grid(0).destroyCaches(cacheNames);
            awaitPartitionMapExchange();
        }
    }

    @Test
    @WithSystemProperty(key = IGNITE_SQL_ALLOW_IMPLICIT_PK, value = "true")
    public void testImplicitPkWithRestart() throws Exception {
        querySql("CREATE TABLE integers(i INTEGER)");
        querySql("INSERT INTO integers(i) VALUES(?)", 0);

        stopAllGrids();
        startGridsMultiThreaded(NODES_CNT);

        querySql("INSERT INTO integers(i) VALUES(?)", 1);

        List<List<?>> res = querySql("SELECT * FROM integers");

        assertEquals(2, res.size());

        Set<Integer> resSet = new HashSet<>();

        for (List<?> row : res) {
            assertEquals(1, row.size());

            resSet.add((Integer)F.first(row));
        }
    }

    @Test
    @WithSystemProperty(key = IGNITE_SQL_ALLOW_IMPLICIT_PK, value = "true")
    public void testImplicitPk() {
        querySql("CREATE TABLE integers(i INTEGER)");

        int cnt = 5;
        Set<Integer> expSet = new HashSet<>();

        for (int n = 0; n < cnt; n++) {
            expSet.add(n);

            querySql("INSERT INTO integers(i) VALUES(?)", n);
        }

        List<List<?>> res = querySql("SELECT * FROM integers");

        assertEquals(cnt, res.size());

        Set<Integer> resSet = new HashSet<>();

        for (List<?> row : res) {
            assertEquals(1, row.size());

            resSet.add((Integer)F.first(row));
        }

        assertEquals(expSet, resSet);
    }

    private List<List<?>> querySql(String sql, Object... args) {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(sql).setArgs(args))) {
            assertNotNull(cur);

            return cur.getAll();
        }
    }
}

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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class CreateTableImplicitPkTest extends GridCommonAbstractTest {
    private static final int NODES_CNT = 2;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCacheConfiguration(cacheCfg(DEFAULT_CACHE_NAME));
    }

    private CacheConfiguration<?, ?> cacheCfg(String name) {
        return new CacheConfiguration<>(name).setBackups(1);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_CNT);
    }

    @Test
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

    private List<List<?>> querySql(String sql, Object ... args) {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(sql).setArgs(args))) {
            assertNotNull(cur);

            return cur.getAll();
        }
    }
}

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

package org.apache.ignite.internal.processors.query;

import java.sql.SQLException;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

public class SqlQueryPriorityTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (String cacheName : grid(0).cacheNames())
            grid(0).cache(cacheName).destroy();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setQueryThreadPoolSize(1);

        return cfg;
    }

    @Test
    public void test0() throws Exception {
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("cache")
                .setSqlSchema("PUBLIC")
                .setSqlFunctionClasses(SqlQueryPriorityTest.class);

        IgniteCache<?, ?> cache = grid(0).getOrCreateCache(cacheCfg);

        sql((byte)0, cache, "CREATE TABLE Person (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))");
        sql((byte)0, cache, "INSERT INTO Person (ID, NAME) VALUES (3, 'Emma'), (2, 'Ann'), (1, 'Ed')");

        IgniteInternalFuture f1 = GridTestUtils.runAsync(() -> {
            sql((byte)0, cache, "SELECT sleep_func(1000), NAME, ID FROM Person ORDER BY CAST(ID AS LONG)").getAll();
            return null;
        });

        Thread.sleep(100);

        IgniteInternalFuture f2 = GridTestUtils.runAsync(() -> {
            sql((byte)1, cache, "SELECT sleep_func(1000), NAME, ID FROM Person ORDER BY CAST(ID AS LONG)").getAll();
            return null;
        });

        IgniteInternalFuture f3 = GridTestUtils.runAsync(() -> {
            sql((byte)2, cache, "SELECT sleep_func(1000), NAME, ID FROM Person ORDER BY CAST(ID AS LONG)").getAll();
            return null;
        });

        f1.listen(new IgniteInClosure<IgniteInternalFuture>() {
            @Override
            public void apply(IgniteInternalFuture igniteInternalFuture) {
                f3.listen(new IgniteInClosure<IgniteInternalFuture>() {
                    @Override
                    public void apply(IgniteInternalFuture igniteInternalFuture) {
                        assertFalse(f2.isDone());
                    }
                });
            }
        });

        f1.get();
        f3.get();

        System.err.println();
    }

    @QuerySqlFunction
    public static int sleep_func(int v) {
        try {
            Thread.sleep(v);
        }
        catch (InterruptedException ignored) {
            // No-op
        }
        return v;
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(byte priority, IgniteCache<?, ?> cache, String sql, Object... args) {
        return cache.query(new SqlFieldsQuery(sql).setArgs(args).setPriority(priority));
    }
}

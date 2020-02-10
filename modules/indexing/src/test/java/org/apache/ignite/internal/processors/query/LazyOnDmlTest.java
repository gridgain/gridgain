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

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.H2LocalResultFactory;
import org.apache.ignite.internal.processors.query.h2.H2ManagedLocalResult;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.result.LocalResult;
import org.h2.result.LocalResultImpl;
import org.junit.Ignore;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Tests for lazy mode for DML queries.
 */
public class LazyOnDmlTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEY_CNT = 10;

    /** Query local results. */
    static final List<H2ManagedLocalResult> localResults = Collections.synchronizedList(new ArrayList<>());

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_H2_LOCAL_RESULT_FACTORY, TestH2LocalResultFactory.class.getName());

        startGrid();
   }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        System.clearProperty(IgniteSystemProperties.IGNITE_H2_LOCAL_RESULT_FACTORY);

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteCache<Long, Long> c = grid().createCache(new CacheConfiguration<Long, Long>()
            .setName("test")
            .setSqlSchema("TEST")
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class, Long.class)
                .setTableName("test")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("val", Long.class.getName(), null)
                .setKeyFieldName("id")
                .setValueFieldName("val")
            ))
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, 10)));

        for (long i = 0; i < KEY_CNT; ++i)
            c.put(i, i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (String cache : grid().cacheNames())
            grid().cache(cache).destroy();

        super.afterTest();
    }

    /**
     */
    @Test
    public void testUpdateWithoutReduce() {
        sql("UPDATE test SET val = val + 1");

        assertEquals(0, localResults.size());
    }

    /**
     */
    @Test
    public void testDeleteWithoutReduce() {
        sql("DELETE FROM test WHERE val >= 0");

        assertEquals(0, localResults.size());
    }

    /**
     */
    @Test
    @Ignore("https://ggsystems.atlassian.net/browse/GG-27502")
    public void testUpdateWithReduce() {
        sql("UPDATE test SET val = val + AVG(val)");

        assertEquals(0, localResults.size());
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object ... args) {
        return sql(grid(), sql, args);
    }

    /**
     * @param ign Node.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(IgniteEx ign, String sql, Object ... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLazy(true)
            .setSchema("TEST")
            .setArgs(args), false);
    }

    /**
     * Local result factory for test.
     */
    public static class TestH2LocalResultFactory extends H2LocalResultFactory {
        /** {@inheritDoc} */
        @Override public LocalResult create(Session ses, Expression[] expressions, int visibleColCnt, boolean system) {
            if (system)
                return new LocalResultImpl(ses, expressions, visibleColCnt);

            H2MemoryTracker memoryTracker = ses.queryMemoryTracker();

            if (memoryTracker != null) {
                H2ManagedLocalResult res = new H2ManagedLocalResult(ses, memoryTracker, expressions, visibleColCnt) {
                    @Override public void onClose() {
                        // Just prevent 'rows' from being nullified for test purposes.

                        memoryTracker().released(memoryReserved());
                    }
                };

                localResults.add(res);

                return res;
            }

            return new H2ManagedLocalResult(ses, null, expressions, visibleColCnt);
        }

        /** {@inheritDoc} */
        @Override public LocalResult create() {
            throw new NotImplementedException();
        }
    }
}

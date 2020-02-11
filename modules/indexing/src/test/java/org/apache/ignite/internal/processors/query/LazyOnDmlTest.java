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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Tests for lazy mode for DML queries.
 */
@RunWith(Parameterized.class)
public class LazyOnDmlTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEY_CNT = 10_000;

    /** Query local results. */
    static final List<H2ManagedLocalResult> localResults = Collections.synchronizedList(new ArrayList<>());

    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameter(1)
    public CacheMode cacheMode;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "atomicityMode={0}, cacheMode={1}")
    public static Collection parameters() {
        Set<Object[]> paramsSet = new LinkedHashSet<>();

        Object[] paramTemplate = new Object[2];

        for (CacheAtomicityMode atomicityMode : CacheAtomicityMode.values()) {
            paramTemplate = Arrays.copyOf(paramTemplate, paramTemplate.length);

            paramTemplate[0] = atomicityMode;

            for (CacheMode cacheMode : new CacheMode[] {CacheMode.PARTITIONED, CacheMode.REPLICATED}) {
                Object[] params = Arrays.copyOf(paramTemplate, paramTemplate.length);

                params[1] = cacheMode;

                paramsSet.add(params);
            }
        }

        return paramsSet;
    }


    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_H2_LOCAL_RESULT_FACTORY, TestH2LocalResultFactory.class.getName());

        startGrids(3);
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

        IgniteCache<Long, Long> c = grid(0).createCache(new CacheConfiguration<Long, Long>()
            .setName("test")
            .setSqlSchema("TEST")
            .setAtomicityMode(atomicityMode)
            .setCacheMode(cacheMode)
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class, Long.class)
                .setTableName("test")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("val", Long.class.getName(), null)
                .setKeyFieldName("id")
                .setValueFieldName("val")
                .setIndexes(Collections.singletonList(
                    new QueryIndex("val")
                ))
            ))
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, 10)));

        try (IgniteDataStreamer streamer = grid(0).dataStreamer("test")) {
            for (long i = 0; i < KEY_CNT; ++i)
                streamer.addData(i, i);
        }

        localResults.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (String cache : grid(0).cacheNames())
            grid(0).cache(cache).destroy();

        super.afterTest();
    }

    /**
     */
    @Test
    public void testUpdateNotLazy() {
        List<List<?>> res = sql("UPDATE test SET val = val + 1 WHERE val >= 0").getAll();

        // Check that all rows updates only ones.
        assertEquals((long)KEY_CNT, res.get(0).get(0));

        if (atomicityMode == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
            assertEquals(0, localResults.size());
        else if (cacheMode == CacheMode.REPLICATED)
            assertEquals(1, localResults.size());
        else
            assertEquals(3, localResults.size());
    }

    /**
     */
    @Test
    public void testUpdateLazy() {
        List<List<?>> res = sql("UPDATE test SET val = val + 1").getAll();

        // Check that all rows updates only ones.
        assertEquals((long)KEY_CNT, res.get(0).get(0));

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
        return sql(grid(0), sql, args);
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
            .setPageSize(1)
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

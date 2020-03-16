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
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.H2LocalResultFactory;
import org.apache.ignite.internal.processors.query.h2.H2ManagedLocalResult;
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
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class.getName(), "testVal")
                .setTableName("test")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("val0", Long.class.getName(), null)
                .addQueryField("val1", Long.class.getName(), null)
                .addQueryField("val2", Long.class.getName(), null)
                .setKeyFieldName("id")
                .setIndexes(Collections.singletonList(
                    new QueryIndex(Arrays.asList("val0", "val1"), QueryIndexType.SORTED)
                ))
            ))
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, 10)));

        try (IgniteDataStreamer streamer = grid(0).dataStreamer("test")) {
            for (long i = 0; i < KEY_CNT; ++i) {
                BinaryObjectBuilder bob = grid(0).binary().builder("testVal");

                bob.setField("val0", i);
                bob.setField("val1", i);
                bob.setField("val2", i);

                streamer.addData(i, bob.build());
            }
        }

        sql("CREATE TABLE table1 (id INT PRIMARY KEY, col0 INT, col1 VARCHAR (100))");

        sql("INSERT INTO table1 (id, col0, col1) " +
            "SELECT 1, 11, 'FIRST' " +
            "UNION ALL " +
            "SELECT 11,12, 'SECOND' " +
            "UNION ALL " +
            "SELECT 21, 13, 'THIRD' " +
            "UNION ALL " +
            "SELECT 31, 14, 'FOURTH'");

        sql("CREATE TABLE  table2 (id INT PRIMARY KEY, col0 INT, col1 VARCHAR (100))");

        sql("INSERT INTO table2 (id, col0, col1) " +
            "SELECT 1, 21, 'TWO-ONE' " +
            "UNION ALL " +
            "SELECT 11, 22, 'TWO-TWO' " +
            "UNION ALL " +
            "SELECT 21, 23, 'TWO-THREE' " +
            "UNION ALL " +
            "SELECT 31, 24, 'TWO-FOUR'");

        for (H2ManagedLocalResult res : localResults) {
            if (res.memoryTracker() != null)
                res.memoryTracker().close();
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
        checkUpdateNotLazy("UPDATE test SET val0 = val0 + 1 WHERE val0 >= 0");
        checkUpdateNotLazy("UPDATE test SET val1 = val1 + 1 WHERE val0 >= 0");
    }

    /**
     */
    public void checkUpdateNotLazy(String sql) {
        try {
            List<List<?>> res = sql(sql).getAll();

            // Check that all rows updates only ones.
            assertEquals((long)KEY_CNT, res.get(0).get(0));

            if (atomicityMode == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
                assertEquals(0, localResults.size());
            else if (cacheMode == CacheMode.REPLICATED)
                assertEquals(1, localResults.size());
            else
                assertEquals(3, localResults.size());

        }
        finally {
            for (H2ManagedLocalResult res : localResults) {
                if (res.memoryTracker() != null)
                    res.memoryTracker().close();
            }

            localResults.clear();
        }
    }

    /**
     */
    @Test
    public void testUpdateLazy() {
        checkUpdateLazy("UPDATE test SET val0 = val0 + 1");
        checkUpdateLazy("UPDATE test SET val2 = val2 + 1 WHERE val2 >= 0");
        checkUpdateLazy("UPDATE test SET val0 = val0 + 1 WHERE val1 >= 0");
    }

    /**
     */
    public void checkUpdateLazy(String sql) {
        List<List<?>> res = sql(sql).getAll();

        // Check that all rows updates only ones.
        assertEquals((long)KEY_CNT, res.get(0).get(0));

        assertEquals(0, localResults.size());
    }

    /**
     */
    @Test
    public void testDeleteWithoutReduce() {
        List<List<?>> res = sql("DELETE FROM test WHERE val0 >= 0").getAll();

        assertEquals((long)KEY_CNT, res.get(0).get(0));

        assertEquals(0, localResults.size());
    }

    /**
     */
    @Test
    @Ignore("https://ggsystems.atlassian.net/browse/GG-27502")
    public void testUpdateWithReduce() {
        List<List<?>> res = sql("UPDATE test SET val0 = val0 + AVG(val0)").getAll();

        assertEquals((long)KEY_CNT, res.get(0).get(0));

        assertEquals(0, localResults.size());
    }

    /**
     */
    @Test
    public void testUpdateFromSubqueryLazy() {
        List<List<?>> res;

        res = sql("UPDATE table1 " +
            "SET (col0, col1) = " +
            "   (SELECT table2.col0, table2.col1 FROM table2 WHERE table2.id = table1.id)" +
            "WHERE table1.id in (21, 31)").getAll();

        assertEquals(2L, res.get(0).get(0));

        assertEquals(0, localResults.size());

        res = sql("UPDATE table1 " +
            "SET (col0, col1) = " +
            "   (SELECT table2.col0, table2.col1 FROM table2 WHERE table2.id = table1.id) " +
            "WHERE exists (select * from table2 where table2.id = table1.id) " +
            "AND table1.id in (21, 31)").getAll();

        assertEquals(2L, res.get(0).get(0));

        assertEquals(0, localResults.size());
    }

    /**
     */
    @Test
    public void testUpdateValueField() {
        sql("CREATE TABLE TEST2 (id INT PRIMARY KEY, val INT) " +
            "WITH\"WRAP_VALUE=false\"");

        sql("INSERT INTO TEST2  VALUES (0, 0), (1, 1), (2, 2)");

        // 'val' field is the alias for _val. There is index for _val.
        List<List<?>> res = sql("UPDATE TEST2 SET _val = _val + 1 WHERE val >=0").getAll();

        assertEquals(3L, res.get(0).get(0));

        assertFalse(localResults.isEmpty());
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

            if (ses.memoryTracker() != null) {
                H2ManagedLocalResult res = new H2ManagedLocalResult(ses, expressions, visibleColCnt) {
                    @Override public void onClose() {
                        // Just prevent 'rows' from being nullified for test purposes.
                    }
                };

                localResults.add(res);

                return res;
            }

            return new H2ManagedLocalResult(ses, expressions, visibleColCnt);
        }

        /** {@inheritDoc} */
        @Override public LocalResult create() {
            throw new NotImplementedException();
        }
    }
}

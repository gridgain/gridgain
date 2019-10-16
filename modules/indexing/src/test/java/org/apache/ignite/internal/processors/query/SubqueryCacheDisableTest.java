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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test verifies that optimization can be disabled with system property.
 */
public class SubqueryCacheDisableTest extends AbstractIndexingCommonTest {
    /** Test schema. */
    private static final String TEST_SCHEMA = "TEST";

    /** Outer table name. */
    private static final String OUTER_TBL = "OUTER_TBL";

    /** Inner table name. */
    private static final String INNER_TBL = "INNER_TBL";

    /** Outer table size. */
    private static final int OUTER_SIZE = 10;

    /** Inner table size. */
    private static final int INNER_SIZE = 50;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_H2_OPTIMIZE_REUSE_RESULTS, "false");

        startGrid(0);

        createSchema();

        populateData();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IgniteSystemProperties.IGNITE_H2_OPTIMIZE_REUSE_RESULTS);
    }

    /** */
    private void createSchema() {
        createCache(OUTER_TBL, Collections.singleton(new QueryEntity(Long.class.getTypeName(), "A_VAL")
            .setTableName(OUTER_TBL)
            .addQueryField("ID", Long.class.getName(), null)
            .addQueryField("JID", Long.class.getName(), null)
            .addQueryField("VAL", Long.class.getName(), null)
            .setKeyFieldName("ID")
        ));

        createCache(INNER_TBL, Collections.singleton(new QueryEntity(Long.class.getName(), "B_VAL")
            .setTableName(INNER_TBL)
            .addQueryField("ID", Long.class.getName(), null)
            .addQueryField("A_JID", Long.class.getName(), null)
            .addQueryField("VAL0", String.class.getName(), null)
            .setKeyFieldName("ID")
        ));
    }

    /**
     * @param name Name.
     * @param qryEntities Query entities.
     */
    private void createCache(String name, Collection<QueryEntity> qryEntities) {
        grid(0).createCache(
            new CacheConfiguration<Long, Long>()
                .setName(name)
                .setSqlSchema(TEST_SCHEMA)
                .setSqlFunctionClasses(SubqueryCacheTest.TestSQLFunctions.class)
                .setQueryEntities(qryEntities)
        );
    }

    /**
     * Fills caches with test data.
     */
    @SuppressWarnings("unchecked")
    private void populateData() {
        IgniteCache cacheA = grid(0).cache(OUTER_TBL);

        for (long i = 0; i < OUTER_SIZE; ++i) {
            cacheA.put(i, grid(0).binary().builder("A_VAL")
                .setField("JID", i)
                .setField("VAL", i)
                .build());
        }

        IgniteCache cacheB = grid(0).cache(INNER_TBL);

        for (long i = 0; i < INNER_SIZE; ++i)
            cacheB.put(i, grid(0).binary().builder("B_VAL")
                .setField("A_JID", i)
                .setField("VAL0", String.format("val%03d", i))
                .build());

    }

    /**
     * Ensure that optimization is disabled when property
     * {@link IgniteSystemProperties#IGNITE_H2_OPTIMIZE_REUSE_RESULTS}
     * is set to {@code false}.
     */
    @Test
    public void testOptimizationDisabled() {
        SubqueryCacheTest.TestSQLFunctions.CALL_CNT.set(0);

        FieldsQueryCursor<List<?>> res = sql("SELECT * FROM " + OUTER_TBL + " WHERE JID IN (SELECT ID FROM "
            + INNER_TBL + " WHERE det_foo(ID) = ID)");

        Assert.assertEquals(0, SubqueryCacheTest.TestSQLFunctions.CALL_CNT.get());

        res.getAll();

        Assert.assertEquals(INNER_SIZE * OUTER_SIZE, SubqueryCacheTest.TestSQLFunctions.CALL_CNT.get());
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setSchema(TEST_SCHEMA)
            .setLazy(true)
            .setArgs(args), false);
    }
}

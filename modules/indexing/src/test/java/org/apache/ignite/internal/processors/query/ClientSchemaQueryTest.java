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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.h2.result.LazyResult;
import org.h2.result.ResultInterface;
import org.junit.Test;

/**
 * Tests for local query execution in lazy mode.
 */
public class ClientSchemaQueryTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEY_CNT = 10;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setClientMode(igniteInstanceName.startsWith("cli"));

        if (!igniteInstanceName.startsWith("cli")) {
            cfg.setCacheConfiguration(new CacheConfiguration<Long, Long>()
                .setName("testcfg")
                .setAffinity(new RendezvousAffinityFunction(false, 10)));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid("srv");

//        IgniteCache<Long, Long> c = grid("srv").createCache(new CacheConfiguration<Long, Long>()
//            .setName("test")
//            .setSqlSchema("TEST")
//            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class, Long.class)
//                .setTableName("test")
//                .addQueryField("id", Long.class.getName(), null)
//                .addQueryField("val", Long.class.getName(), null)
//                .setKeyFieldName("id")
//                .setValueFieldName("val")
//            ))
//            .setAffinity(new RendezvousAffinityFunction(false, 10)));
//
//        for (long i = 0; i < KEY_CNT; ++i)
//            c.put(i, i);

        startGrid("cli");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     */
    @Test
    public void test() {
//        sql(grid("srv"), "TEST", "CREATE TABLE NEWTBL0_SRV(ID INT PRIMARY KEY, VAL VARCHAR)");
//
//        sql(grid("cli"), "TEST", "CREATE TABLE NEWTBL0_CLI(ID INT PRIMARY KEY, VAL VARCHAR)");

//        sql(grid("srv"), null, "CREATE TABLE \"testcfg\".NEWTBL1_SRV(ID INT PRIMARY KEY, VAL VARCHAR)");

        sql(grid("cli"), "testcfg", "CREATE TABLE NEWTBL1_CLI(ID INT PRIMARY KEY, VAL VARCHAR)");

        sql(grid("cli"), "testcfg", "INSERT INTO \"testcfg\".NEWTBL1_CLI VALUES (1, '1')");

    }

    /**
     * @param ign Ignite.
     * @param schema SQL schema.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(IgniteEx ign, String schema,  String sql, Object ... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLazy(true)
            .setSchema(schema)
            .setArgs(args), false);
    }
}

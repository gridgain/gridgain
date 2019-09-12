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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.h2.api.AggregateFunction;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for registration custom aggregation functions.
 */
public class SqlJavaAggregationTest extends AbstractIndexingCommonTest {

    /**
     * @throws Exception If error.
     */
    @Test
    public void javaAggregationFunction() throws Exception {
        IgniteEx grid = startGrid(0);

        IgniteCache<?,?> cache = grid.getOrCreateCache("person");

        IgniteH2Indexing indexing = (IgniteH2Indexing) grid.context().query().getIndexing();

        indexing.connections().executeStatement(null,"CREATE AGGREGATE IF NOT EXISTS ACCUMULATE FOR \"" + AccumulateFunction.class.getName() + "\"");

        SqlFieldsQuery qry = new SqlFieldsQuery("select ACCUMULATE(id) from table(id varchar = (1,2,3,4,5))");

        List<List<?>> all = cache.query(qry).getAll();

        assertEquals(1, all.size());
        assertEquals(1, all.get(0).size());

        Object result = all.get(0).get(0);

        assertTrue(result instanceof Object[]);
        assertEquals(5, ((Object[])result).length );
    }


    /**
     * Test aggregation function for collecting objects.
     */
    public static class AccumulateFunction implements AggregateFunction {

        /** */
        List<Object> result = new ArrayList<>();

        @Override
        public void init(Connection conn) throws SQLException {
        }

        @Override
        public int getType(int[] inputTypes) throws SQLException {
            return 0;
        }

        @Override
        public void add(Object value) throws SQLException {
            result.add(value);
        }

        @Override
        public Object getResult() throws SQLException {
            return result.toArray();
        }
    }
}

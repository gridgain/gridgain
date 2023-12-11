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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlTable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class CacheQuerySqlTableTest extends GridCommonAbstractTest {
    @QuerySqlTable(keyFieldName = "id", valueFieldName = "all_values", name = "query_table_1")
    private static class QueryTable1 {
    }

    @Test
    public void testCreateWithAllProperties() {
        CacheConfiguration<Long, QueryTable1> cacheConfiguration = new CacheConfiguration<Long, QueryTable1>()
                .setName("QUERYENTITYTEST")
                .setIndexedTypes(Long.class, QueryTable1.class);

        QueryEntity qe = (QueryEntity) cacheConfiguration.getQueryEntities().toArray()[0];
        assertEquals("id", qe.getKeyFieldName());
        assertEquals("all_values", qe.getValueFieldName());
        assertEquals("query_table_1", qe.getTableName());
    }

    @QuerySqlTable(name = "query_table_2", valueFieldName = "all_values_2")
    private static class QueryTable2 {
        @QuerySqlField
        private String field1;
    }

    @Test
    public void testCreateWithTableNameAndValue() {
        CacheConfiguration<Long, QueryTable2> cacheConfiguration = new CacheConfiguration<Long, QueryTable2>()
                .setName("QUERYENTITYTEST")
                .setIndexedTypes(Long.class, QueryTable2.class);

        QueryEntity qe = (QueryEntity) cacheConfiguration.getQueryEntities().toArray()[0];
        assertEquals("query_table_2", qe.getTableName());
        assertEquals("all_values_2", qe.getValueFieldName());
        assertNull(qe.getKeyFieldName());
    }

    @QuerySqlTable(keyFieldName = "id")
    private static class QueryTable3 {
    }

    @Test
    public void testCreateWithTableKey() {
        CacheConfiguration<Long, QueryTable3> cacheConfiguration = new CacheConfiguration<Long, QueryTable3>()
                .setName("QUERYENTITYTEST")
                .setIndexedTypes(Long.class, QueryTable3.class);

        QueryEntity qe = (QueryEntity) cacheConfiguration.getQueryEntities().toArray()[0];
        assertEquals("id", qe.getKeyFieldName());
        assertNull(qe.getTableName());
        assertNull(qe.getValueFieldName());
    }

}

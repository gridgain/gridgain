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

import java.sql.Date;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests to verify the addition of columns in tables.
 */
public class IgniteSqlAddColumnTest extends GridCommonAbstractTest {
    /** Cache. */
    private static IgniteCache<Object, Object> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteEx node = startGrid(0);

        cache = node.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.query(
            new SqlFieldsQuery("Create table Person (id int PRIMARY KEY, name varchar, birthday timestamp)")
        ).getAll();

        SqlFieldsQuery insertQry = new SqlFieldsQuery("INSERT INTO Person (id, name, birthday) VALUES (?, ?, ?)");

        for (int i = 0; i < 10; i++) {
            Date birthday = new Date(365L * 24 * 60 * 60 * 1000 * i);
            insertQry.setArgs(i, "name" + i, birthday);
            cache.query(insertQry).getAll();
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setSqlSchema(DFLT_SCHEMA));

        return cfg;
    }

    /**
     * Check that there will be an error in the case of deleting the column and
     * its return, but with a different type.
     */
    @Test
    public void testChangeColumnType() {
        cache.query(new SqlFieldsQuery("alter table Person drop column birthday")).getAll();

        assertThrows(
            log,
            () -> cache.query(new SqlFieldsQuery("alter table Person add column birthday date")).getAll(),
            CacheException.class,
            "Column already exists: with a different type."
        );
    }

    /**
     * Check that there will be no error if you delete a column and
     * return it with the same type.
     */
    @Test
    public void testReturnColumn() {
        cache.query(new SqlFieldsQuery("alter table Person drop column birthday")).getAll();
        cache.query(new SqlFieldsQuery("alter table Person add column birthday timestamp")).getAll();
    }

    /**
     * Check that when adding a new column there will be no error.
     */
    @Test
    public void testAddNewColumn() {
        cache.query(new SqlFieldsQuery("alter table Person add column surname varchar")).getAll();
    }
}

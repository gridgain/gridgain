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

package org.apache.ignite.internal.processors.query.oom;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * TODO: all possible data types and nulls
 * TODO: BLOBS/CLOBS??
 * TODO: sorted/unsorted
 * TODO: UNION/EXCEPT
 * TODO: createShallowCopy (query caching)
 * TODO: client/server
 * TODO: multinode/multithreaded
 * TODO: multiple spills during one query
 * TODO: file deleted in the case of query exception/kill/etc
 * TODO: DISTINCT ON(...)/DISTINCT/EXCEPT/INTERSECT
 * TODO: lazy/not lazy, local/distributed
 * TODO: test directory cleaned on startup
 * TODO: ADD logging
 *
 * Test for the intermediate query results disk offloading (disk spilling).
 */
@WithSystemProperty(key = "IGNITE_SQL_FAIL_ON_QUERY_MEMORY_LIMIT_EXCEED", value = "false")
public class DiskSpillingTest extends GridCommonAbstractTest {
    /** */
    private static final int PERS_CNT = 1000;

    /** */
    private static final int DEPS_CNT = 100;

    /** */
    private static final long SMALL_MEM_LIMIT = 2048;

    /** */
    private static final long HUGE_MEM_LIMIT = Long.MAX_VALUE;

    /** */
    private boolean checkSortOrder;


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Dummy cache.
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cfg.setCacheConfiguration(cache);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        populateData();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        checkSortOrder = false;
    }

    /** */
    @Test
    public void simpleSelect() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT * FROM person");
    }

    /** */
    @Test
    public void simpleSelectWithSort() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT * FROM person ORDER BY id DESC");
    }

    /** */
    @Test
    public void simpleSelectWithDistinct() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT DISTINCT(code) FROM person");
    }

    /** */
    @Test
    public void simpleSelectWithDistinctOrderBy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT DISTINCT code FROM person ORDER BY code");
    }

    /** */
    @Test
    public void simpleSelectWithDistinctOrderByAggregate() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT DISTINCT code, depId " +
            "FROM person ORDER BY CONCAT(depId,code)");
    }

    /** */
    @Test
    public void simpleSubSelect() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT name, depId " +
            "FROM person WHERE depId IN (SELECT id FROM department)");
    }

    /** */
    @Test
    public void simpleSelectWithDistinctOn() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT DISTINCT ON (code) code, depId " +
            "FROM person ORDER BY code, depId");
    }

    /** */
    @Test
    public void simpleJoin() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT p.id, p.name, p.depId, d.title " +
            "FROM person p, department d " +
            " WHERE p.depId = d.id");
    }

    /** */
    @Test
    public void simpleUnion() {
        assertInMemoryAndOnDiskSameResults(false,
            "SELECT id, name, code, depId FROM person WHERE depId < 10 " +
                " UNION " +
                "SELECT id, name, code, depId FROM person WHERE depId > 5 ");
    }

    /** */
    @Test
    public void simpleExcept() {
        assertInMemoryAndOnDiskSameResults(false,
            "SELECT id, name, code, depId FROM person WHERE depId >= 0 " +
            " EXCEPT " +
            "SELECT id, name, code, depId FROM person WHERE depId > 0 ");
    }

    /** */
    @Test
    public void simpleIntersect() {
        assertInMemoryAndOnDiskSameResults(false,
            "SELECT id, name, code, depId FROM person WHERE depId < 10 " +
                " INTERSECT " +
                "SELECT id, name, code, depId FROM person WHERE depId > 5 ");
    }


    public void assertInMemoryAndOnDiskSameResults(boolean lazy, String sql) {
        long startOnDisk = System.currentTimeMillis();

        List<List<?>> onDiskRes = runSql(sql, lazy, SMALL_MEM_LIMIT);

        long startInMem = System.currentTimeMillis();

        List<List<?>> inMemRes = runSql(sql, lazy, HUGE_MEM_LIMIT);

        long finish = System.currentTimeMillis();

        assertFalse(inMemRes.isEmpty());

        System.out.println("with disk spill=" + (startInMem - startOnDisk) + ", inMemory time=" + (finish - startInMem));

        if (!checkSortOrder) {
            fixSortOrder(onDiskRes);
            fixSortOrder(inMemRes);
        }

        assertEqualsCollections(inMemRes, onDiskRes);
    }


    private void populateData() {
        // Persons
        runDdlDml("CREATE TABLE person (" +
            "id BIGINT PRIMARY KEY, " +
            "name VARCHAR, " +
            "depId INT, " +
            "code CHAR(3))");

        for (int i = 0; i < PERS_CNT; i++) {
            runDdlDml("INSERT INTO person (id, name, depId,  code) VALUES (?, ?, ?, ?)",
                i, "Vasya" + i, i % DEPS_CNT, "p" + i % 31);
        }

        // Departments
        runDdlDml("CREATE TABLE department (" +
            "id INT PRIMARY KEY, " +
            "title VARCHAR_IGNORECASE)");

        for (int i = 0; i < DEPS_CNT; i++) {
            runDdlDml("INSERT INTO department (id, title) VALUES (?, ?)", i, "IT" + i);
        }
    }

    private List<List<?>> runSql(String sql, boolean lazy, long memLimit) {
        return grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQueryEx(sql, null)
            .setMaxMemory(memLimit)
            .setLazy(lazy)
            .setLocal(true)// TODO local/not local
        ).getAll();
    }

    private List<List<?>> runDdlDml(String sql, Object... args) {
        return grid(0)
            .cache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQueryEx(sql, null)
                .setArgs(args)
            ).getAll();
    }


    private void fixSortOrder(List<List<?>> onDiskRes) {
        Collections.sort(onDiskRes, new Comparator<List<?>>() {
            @Override public int compare(List<?> l1, List<?> l2) {
                for (int i = 0; i < l1.size(); i++) {
                    Object o1 = l1.get(i);
                    Object o2 = l2.get(i);

                    if (o1.hashCode() == o2.hashCode())
                        continue;

                    return o1.hashCode() > o2.hashCode() ? 1 : -1;
                }

                return 0;
            }
        });
    }
}

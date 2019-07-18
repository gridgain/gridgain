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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.DISK_SPILL_DIR;

/**
 *
 *
 *
 * // Extra test
 * TODO: test directory cleaned on startup
 * TODO: Global quota
 * TODO: file deleted in the case of query exception/kill/etc
 * TODO: multithreaded
 *
 * // Before start
 * TODO: single node/multiple nodes
 * TODO: parallelism
 * TODO: persistence and in-memory
 *
 *
 * // Coding
 * TODO: scale
 * TODO: ADD logging
 *
 * Test for the intermediate query results disk offloading (disk spilling).IgniteSystemProperties.IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE
 */
@WithSystemProperty(key = "IGNITE_SQL_FAIL_ON_QUERY_MEMORY_LIMIT_EXCEED", value = "false")
@WithSystemProperty(key = "IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE", value = "2048")
@RunWith(Parameterized.class)
public class DiskSpillingTest extends GridCommonAbstractTest {
    /** */
    private static final int PERS_CNT = 1000;

    /** */
    private static final int DEPS_CNT = 100;

    /** */
    private static final long SMALL_MEM_LIMIT = 4096;

    /** */
    private static final long HUGE_MEM_LIMIT = Long.MAX_VALUE;

    /** */
    private boolean checkSortOrder;

    /** */
    @Parameterized.Parameter(0)
    public boolean fromClient;

    /** */
    @Parameterized.Parameter(1)
    public boolean local;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "fromClient={0}, local={1}")
    public static Collection parameters() {
        return Arrays.asList(new Object[][] {
            //client, local
            { false, false },
            { false, true },
            { true,  false },
        });
    }


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

        startGrids(nodeCount());

        startGrid(getConfiguration("client").setClientMode(true));

        populateData();
    }

    protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        checkSortOrder = false;
    }


    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        FileUtils.cleanDirectory(getWorkDir().toFile());
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
    public void simpleSelectWithSortLazy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(true, "SELECT * FROM person ORDER BY id ASC, code ASC");
    }

    /** */
    @Test
    public void simpleSelectWithDistinct() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT DISTINCT code FROM person");
    }

    /** */
    @Test
    public void simpleSelectWithDistinctLazy() {
        assertInMemoryAndOnDiskSameResults(true, "SELECT DISTINCT depId, code FROM person");
    }

    /** */
    @Test
    public void simpleSelectWithDistinctOrderBy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT DISTINCT code FROM person ORDER BY code");
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-21599")
    @Test
    public void simpleSelectWithDistinctOrderByLazy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(true, "SELECT DISTINCT ON (code, depId) salary  " +
            "FROM person ORDER BY code, salary DESC");
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-21595")
    @Test
    public void simpleSelectWithDistinctOrderByAggregate() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT DISTINCT code, depId " +
            "FROM person ORDER BY CONCAT(depId, code)");
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-21595")
    @Test
    public void simpleSelectWithDistinctOrderByAggregateLazy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(true, "SELECT DISTINCT code, depId, temperature " +
            "FROM person ORDER BY CONCAT(depId, code)");
    }

    /** */
    @Test
    public void simpleSubSelect() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT * " +
            "FROM person WHERE depId IN (SELECT id FROM department)");
    }

    /** */
    @Test
    public void simpleSubSelectLazy() {
        assertInMemoryAndOnDiskSameResults(true, "SELECT * " +
            "FROM person WHERE depId IN (SELECT id FROM department) ORDER BY salary DESC");
    }

    /** */
    @Test
    public void simpleSelectWithDistinctOn() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT DISTINCT ON (code) code, depId " +
            "FROM person ORDER BY code, depId");
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-21599")
    @Test
    public void simpleSelectWithDistinctOnLazy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(true, "SELECT DISTINCT ON (code, depId) age " +
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
    public void simpleJoinLazy() {
        assertInMemoryAndOnDiskSameResults(true, "SELECT p.id, p.name, p.depId, d.title " +
            "FROM person p, department d " +
            " WHERE p.depId = d.id AND  (p.id > 10 OR p.id < 10000) ORDER BY p.salary DESC OFFSET 10");
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
    public void simpleUnionLazy() {
        assertInMemoryAndOnDiskSameResults(true,
            "(SELECT DISTINCT id, name, code, depId FROM person WHERE depId < 10  ORDER BY code DESC OFFSET 20)" +
                " UNION " +
                "SELECT id, name, code, depId FROM person WHERE depId > 5 ORDER BY depId DESC LIMIT 200");
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
    public void simpleExceptLazy() {
        assertInMemoryAndOnDiskSameResults(true,
            "SELECT id, name, code, depId FROM person WHERE depId >= 1 " +
                " EXCEPT " +
                "SELECT DISTINCT id, name, code, depId FROM person WHERE depId > 5 ");
    }

    /** */
    @Test
    public void simpleIntersect() {
        assertInMemoryAndOnDiskSameResults(false,
            "SELECT id, name, code, depId FROM person WHERE depId < 10 " +
                " INTERSECT " +
                "SELECT id, name, code, depId FROM person WHERE depId > 5 ");
    }

    /** */
    @Test
    public void simpleIntersectLazy() {
        assertInMemoryAndOnDiskSameResults(true,
            "(SELECT id, name, code, depId FROM person WHERE depId < 10 ORDER BY code, id DESC LIMIT 900 OFFSET 10) " +
                " INTERSECT " +
                "SELECT id, name, code, depId FROM person WHERE depId > 5 ");
    }

    /** */
    @Test
    public void simpleSelectLimitOffset() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT * FROM person ORDER BY name LIMIT 500 OFFSET 100");
    }

    /** */
    @Test
    public void simpleSelectLimitOffsetLazy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(true, "SELECT * FROM person ORDER BY name LIMIT 800 OFFSET 20");
    }

    /** */
    @Test
    public void simpleSelectOffset() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT * FROM person ORDER BY name DESC OFFSET 300");
    }

    /** */
    @Test
    public void simpleSelectOffsetLazy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(true, "SELECT code, male, age, height, salary, tax, weight, " +
            "temperature, time, date, timestamp, uuid FROM person ORDER BY name DESC OFFSET 300");
    }

    /** */
    @Test
    public void simpleSelectLimit() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT * FROM person LIMIT 700");
    }


    /** */
    @Test
    public void simpleSelectLimitLazy() {
        assertInMemoryAndOnDiskSameResults(true, "SELECT DISTINCT" +
            " code, male, age, height, salary, tax, weight FROM person ORDER BY tax DESC LIMIT 700");
    }


    /** */
    @Test
    public void intersectUnionAllLimitOffset() {
        assertInMemoryAndOnDiskSameResults(false,
            "(SELECT *FROM person WHERE depId < 10 " +
                "INTERSECT " +
                "SELECT * FROM person WHERE depId > 5 )" +
                "UNION ALL " +
                "SELECT * FROM person WHERE age > 50 ORDER BY id LIMIT 1000 OFFSET 300 ");
    }

    /** */
    @Test
    public void intersectUnionAllLimitOffsetLazy() {
        assertInMemoryAndOnDiskSameResults(true,
            "((SELECT * FROM person WHERE depId < 20 ORDER BY depId) " +
                "INTERSECT " +
                "SELECT * FROM person WHERE depId > 10 )" +
                "UNION ALL " +
                "SELECT DISTINCT * FROM person WHERE age <100  ORDER BY id LIMIT 10000 OFFSET 20 ");
    }

    /** */
    @Test
    public void intersectJoinAllLimitOffset() {
        assertInMemoryAndOnDiskSameResults(false,
            "(SELECT DISTINCT p.age FROM person p JOIN department d WHERE d.id = p.depId AND depId < 10 " +
                "UNION " +
                "SELECT MAX(height) FROM person WHERE depId > 5  )" +
                "UNION ALL " +
                "SELECT DISTINCT id FROM person WHERE age < (SELECT SUM(id) FROM department WHERE id > 3)  " +
                "UNION " +
                "SELECT DISTINCT salary FROM person p JOIN department d ON p.age=d.id OR p.weight < 60");
    }

    /** */
    @Test
    public void intersectJoinAllLimitOffsetLazy() {
        assertInMemoryAndOnDiskSameResults(true,
            "(SELECT DISTINCT p.age FROM person p JOIN department d WHERE d.id = p.depId AND depId < 44 " +
                "UNION " +
                "SELECT MAX(height) FROM person WHERE depId > 5  )" +
                "UNION ALL " +
                "SELECT DISTINCT id FROM person WHERE age < (SELECT SUM(id) FROM department WHERE id > 2)  " +
                "EXCEPT " +
                "SELECT DISTINCT salary FROM person p JOIN department d ON p.age=d.id OR p.weight > 10");
    }

    /** */
    @Test
    public void simpleGroupBy() {
        assertInMemoryAndOnDiskSameResults(false,
            "SELECT depId, COUNT(*) FROM person GROUP BY depId");
    }

    /** */
    @Test
    public void simpleGroupByLazy() {
        assertInMemoryAndOnDiskSameResults(true,
            "SELECT depId, COUNT(*), SUM(salary) FROM person GROUP BY depId");
    }

    /** */
    private void assertInMemoryAndOnDiskSameResults(boolean lazy, String sql) {
        WatchService watchSvc = null;

        try {
            Path workDir = getWorkDir();

            if (log.isInfoEnabled())
                log.info("workDir=" + workDir.toString());

            watchSvc = FileSystems.getDefault().newWatchService();

            WatchKey watchKey = workDir.register(watchSvc, ENTRY_CREATE, ENTRY_DELETE);

            // In-memory.
            long startInMem = System.currentTimeMillis();

            if (log.isInfoEnabled())
                log.info("Run query in memory.");

            List<List<?>> inMemRes = runSql(sql, lazy, HUGE_MEM_LIMIT);

            List<WatchEvent<?>> dirEvts = watchKey.pollEvents();

            // No files should be created for in-memory mode.
            assertEquals(0, workDir.toFile().list().length);
            assertTrue("Evts:" + dirEvts.stream().map(e ->
                e.kind().toString()).collect(Collectors.joining(", ")), dirEvts.isEmpty());
            assertTrue(workDir.toFile().isDirectory());

            // On disk.
            if (log.isInfoEnabled())
                log.info("Run query with disk offloading.");

            long startOnDisk = System.currentTimeMillis();

            watchKey.reset();

            List<List<?>> onDiskRes = runSql(sql, lazy, SMALL_MEM_LIMIT);

            long finish = System.currentTimeMillis();

            dirEvts = watchKey.pollEvents();

            // Check files have been created but deleted later.
            assertFalse(dirEvts.isEmpty());
            assertTrue(workDir.toFile().isDirectory());
            assertEquals("Files=" + Arrays.toString(workDir.toFile().list()),0, workDir.toFile().list().length);
            assertFalse(inMemRes.isEmpty());

            if (log.isInfoEnabled()) {
                log.info("Spill files events (created + deleted): " + dirEvts.size());
            }

            if (!checkSortOrder) {
                fixSortOrder(onDiskRes);
                fixSortOrder(inMemRes);
            }

            if (log.isInfoEnabled())
                log.info("In-memory time=" + (startOnDisk - startInMem) + ", on-disk time=" + (finish - startOnDisk));

            if (log.isDebugEnabled())
                log.debug("In-memory result:\n" + inMemRes + "\nOn disk result:\n" + onDiskRes);

            assertEqualsCollections(inMemRes, onDiskRes);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            U.closeQuiet(watchSvc);
        }
    }

    /** */
    private void populateData() {
        // Persons
        runDdlDml("CREATE TABLE person (" +
            "id BIGINT PRIMARY KEY, " +
            "name VARCHAR, " +
            "depId SMALLINT, " +
            "code CHAR(3)," +
            "male BOOLEAN," +
            "age TINYINT," +
            "height SMALLINT," +
            "salary INT," +
            "tax DECIMAL(4,4)," +
            "weight DOUBLE," +
            "temperature REAL," +
            "time TIME," +
            "date DATE," +
            "timestamp TIMESTAMP," +
            "uuid UUID, " +
            "nulls INT)");

        for (int i = 0; i < PERS_CNT; i++) {
            runDdlDml("INSERT INTO person (" +
                    "id, " +
                    "name, " +
                    "depId,  " +
                    "code, " +
                    "male, " +
                    "age, " +
                    "height, " +
                    "salary, " +
                    "tax, " +
                    "weight, " +
                    "temperature," +
                    "time," +
                    "date," +
                    "timestamp," +
                    "uuid, " +
                    "nulls) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                i,                    // id
                "Vasya" + i,                // name
                i % DEPS_CNT,               // depId
                "p" + i % 31,               // code
                i % 2,                      // male
                i % 100,                    // age
                150 + (i % 50),             // height
                50000 + i,                  // salary
                i / 1000d,       // tax
                50d + i % 50.0,             // weight
                36.6,                       // temperature
                "20:00:" + i % 60,          // time
                "2019-04-" + (i % 29 + 1),  // date
                "2019-04-04 04:20:08." + i % 900, // timestamp
                "736bc956-090c-40d2-94da-916f2161cda" + i % 10, // uuid
                null);                      // nulls
        }

        // Departments
        runDdlDml("CREATE TABLE department (" +
            "id INT PRIMARY KEY, " +
            "title VARCHAR_IGNORECASE)");

        for (int i = 0; i < DEPS_CNT; i++) {
            runDdlDml("INSERT INTO department (id, title) VALUES (?, ?)", i, "IT" + i);
        }
    }

    /** */
    private List<List<?>> runSql(String sql, boolean lazy, long memLimit) {
        Ignite node = fromClient ? grid("client") : grid(0);
        return node.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQueryEx(sql, null)
            .setMaxMemory(memLimit)
            .setLazy(lazy)
            .setLocal(local)
        ).getAll();
    }

    /** */
    private List<List<?>> runDdlDml(String sql, Object... args) {
        return grid(0)
            .cache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQueryEx(sql, null)
                .setArgs(args)
            ).getAll();
    }

    /** */
    private void fixSortOrder(List<List<?>> res) {
        Collections.sort(res, new Comparator<List<?>>() {
            @Override public int compare(List<?> l1, List<?> l2) {
                for (int i = 0; i < l1.size(); i++) {
                    Object o1 = l1.get(i);
                    Object o2 = l2.get(i);

                    if (o1 == null  ||  o2 == null) {
                        if (o1 == null && o2 == null)
                            return 0;

                        return o1 == null ? 1 : -1;
                    }

                    if (o1.hashCode() == o2.hashCode())
                        continue;

                    return o1.hashCode() > o2.hashCode() ? 1 : -1;
                }

                return 0;
            }
        });
    }

    /** */
    private Path getWorkDir() {
        Path workDir = Paths.get(grid(0).configuration().getWorkDirectory(), DISK_SPILL_DIR);

        workDir.toFile().mkdir();
        return workDir;
    }
}

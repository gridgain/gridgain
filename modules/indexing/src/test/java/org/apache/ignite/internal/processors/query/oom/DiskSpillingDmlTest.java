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
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assume;
import org.junit.Test;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;

/**
 * Tests cases for DML queries spilling.
 */
public class DiskSpillingDmlTest extends DiskSpillingAbstractTest {
    /** */
    private static final String COLS = "id, " +
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
        "nulls ";

    /** */
    private static final String CREATE_NEW_TBL = "CREATE TABLE new_table (" +
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
        "nulls INT) " +
        "WITH \"TEMPLATE=person\"";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op. Test environment wil be set up in the @beforeTest method.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        initGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        destroyGrid();
    }

    /**
     * @throws IOException If failed.
     */
    @Test
    public void testUpdatePlain() throws IOException {
        // Ignored in lazy suite.
        Assume.assumeFalse(GridTestUtils.getFieldValue(SqlFieldsQuery.class, "DFLT_LAZY"));

        testUpdate("UPDATE person " +
            "SET age = age + 1 " +
            "WHERE age > 0");
    }


    /**
     * @throws IOException If failed.
     */
    @Test
    public void testUpdateOrderBy() throws IOException {
        // Ignored in lazy suite.
        Assume.assumeFalse(GridTestUtils.getFieldValue(SqlFieldsQuery.class, "DFLT_LAZY"));

        testUpdate("UPDATE person " +
            "SET age = age + 1 " +
            "WHERE id > 500 " +
            "ORDER BY age");
    }

    /**
     * @throws IOException If failed.
     */
    @Test
    public void testUpdateSubSelect() throws IOException {
        testUpdate("UPDATE person " +
            "SET age = age + 1 " +
            "WHERE age IN (SELECT age FROM person WHERE id < 200) AND age > 0");
    }

    /**
     * @throws IOException If failed.
     */
    @Test
    public void testUpdateDistinct() throws IOException {
        testUpdate("UPDATE person " +
            "SET age = age + 1 " +
            "WHERE age IN (SELECT DISTINCT age FROM person WHERE id > 800)");
    }

    /**
     * Tests update clause works correctly with offloading.
     *
     * @param updateDml DML to run.
     * @throws IOException If failed.
     */
    private void testUpdate(String updateDml) throws IOException {
        assertTrue("Update DML should increment age by 1: " + updateDml, updateDml.contains("SET age = age + 1"));

        List<List<?>> agesBefore = runSql("SELECT age FROM person");

        long affectedRows = runDmlAndCheckOffloading(updateDml);

        List<List<?>> agesAfter = runSql("SELECT age FROM person");

        assertTrue(!agesBefore.isEmpty());
        assertEquals(agesBefore.size(), agesAfter.size());

        int sumAgesBefore = sumAges(agesBefore);
        int sumAgesAfter = sumAges(agesAfter);

        // We had X total sum age in the beginning. Then we updated Y rows and set age = age + 1.
        // So, we should expect that new total age sum is X + Y.
        assertEquals(sumAgesBefore + affectedRows, sumAgesAfter);
        checkMemoryManagerState();
    }

    /**
     * Returns sum of ages from the result set.
     *
     * @param ages Ages result set.
     * @return Sum of ages.
     */
    private Integer sumAges(List<List<?>> ages) {
        return ages.stream().map(l -> l.stream().mapToInt(e -> (Byte)e).sum()).reduce(0, Integer::sum);
    }

    /**
     * @throws IOException If failed.
     */
    @Test
    public void testDeleteSimple() throws IOException {
        // Ignored in lazy suite.
        Assume.assumeFalse(GridTestUtils.getFieldValue(SqlFieldsQuery.class, "DFLT_LAZY"));

        testDelete("DELETE FROM person " +
            "WHERE age > 10");
    }

    /**
     * @throws IOException If failed.
     */
    @Test
    public void testDeleteSubQuery() throws IOException {
        testDelete("DELETE FROM person " +
            "WHERE age IN (SELECT age FROM person WHERE id < 150 AND age < 30 ORDER BY code)");
    }

    /**
     * @throws IOException If failed.
     */
    @Test
    public void testDeleteSubQueryDistinct() throws IOException {
        testDelete("DELETE FROM person " +
            "WHERE age IN (SELECT DISTINCT age FROM person WHERE id > 900 AND age > 70)");
    }

    /**
     * @throws IOException If failed.
     */
    @Test
    public void testDeleteSubQueryUnion() throws IOException {
        testDelete("DELETE FROM person " +
            "WHERE age IN (" +
            "(SELECT age FROM person WHERE depId < 3) " +
            "UNION ALL " +
            "SELECT age FROM person WHERE age < 25  LIMIT 10000 OFFSET 20)");
    }

    /**
     * Check DELETE command works correctly in the case of offloading.
     *
     * @param deleteDml DML command.
     * @throws IOException If failed.
     */
    private void testDelete(String deleteDml) throws IOException {
        Long cntBefore = (Long)runSql("SELECT COUNT(*) FROM person").get(0).get(0);

        long affectedRows = runDmlAndCheckOffloading(deleteDml);

        Long cntAfter = (Long)runSql("SELECT COUNT(*) FROM person").get(0).get(0);

        assertTrue("cntBefore=" + cntBefore + ", cntAfter=" + cntAfter,
            cntBefore != null && cntAfter != null && cntBefore > 0 && cntAfter > 0);

        // X rows was in the beginning. We deleted Y rows and Z rows is left. Expect X = Y + Z.
        assertEquals((long)cntBefore, cntAfter + affectedRows);

        checkMemoryManagerState();
    }

    /**
     * @throws IOException If failed.
     */
    @Test
    public void testInsertSimple() throws IOException { // Ignored in lazy suite.
        Assume.assumeFalse(GridTestUtils.getFieldValue(SqlFieldsQuery.class, "DFLT_LAZY"));

        testInsert("INSERT INTO new_table (" + COLS + ") " +
            " SELECT * FROM person");
    }

    /**
     * @throws IOException If failed.
     */
    @Test
    public void testInsertSorted() throws IOException {
        testInsert("INSERT INTO new_table (" + COLS + ") " +
            " SELECT * FROM person WHERE id < 100 ORDER BY code");
    }

    /**
     * @throws IOException If failed.
     */
    @Test
    public void testInsertDistinct() throws IOException {
        testInsert("INSERT INTO new_table (" + COLS + ") " +
            "SELECT DISTINCT *  FROM person");
    }

    /**
     * @throws IOException If failed.
     */
    @Test
    public void testInsertUnion() throws IOException {
        testInsert("INSERT INTO new_table (" + COLS + ") " +
            "((SELECT * FROM person WHERE depId > 95) " +
            "UNION " +
            "SELECT * FROM person WHERE age > 90  LIMIT 10000 OFFSET 20)");
    }

    /**
     * Check INSERT command works correctly in the case of offloading.
     *
     * @param insertDml DML command.
     * @throws IOException If failed.
     */
    private void testInsert(String insertDml) throws IOException {
        runSql(CREATE_NEW_TBL);

        long affectedRows = runDmlAndCheckOffloading(insertDml);

        Long newTblCnt = (Long)runSql("SELECT COUNT(*) FROM new_table").get(0).get(0);

        assertTrue("newTblCnt=" + newTblCnt, newTblCnt != null && newTblCnt > 0);

        assertEquals(affectedRows, (long)newTblCnt);

        List<List<?>> newTblContent = runSql("SELECT * FROM new_table ORDER BY id");
        List<List<?>> oldTblCopiedContent = runSql("SELECT * FROM person WHERE id IN (SELECT id FROM new_table) ORDER BY id");

        assertEqualsCollections(newTblContent, oldTblCopiedContent);

        checkMemoryManagerState();
    }

    /**
     * Runs arbitrary sql.
     *
     * @param sql SQL.
     * @return Result.
     */
    private List<List<?>> runSql(String sql) {
        return grid(0).cache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQuery(sql))
            .getAll();
    }

    /**
     * Runs DML and ensures that offloading actually happened.
     *
     * @param dml DML command.
     * @return Update rows count.
     * @throws IOException If failed.
     */
    private long runDmlAndCheckOffloading(String dml) throws IOException {
        WatchService watchSvc = FileSystems.getDefault().newWatchService();

        try {
            Path workDir = getWorkDir();

            WatchKey watchKey = workDir.register(watchSvc, ENTRY_CREATE, ENTRY_DELETE);

            List<List<?>> res = grid(0).cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQueryEx(dml, false)
                    .setMaxMemory(SMALL_MEM_LIMIT))
                .getAll();

            List<WatchEvent<?>> dirEvts = watchKey.pollEvents();

            // Check files have been created but deleted later.
            assertFalse("Disk spilling not happened.", dirEvts.isEmpty());

            assertWorkDirClean();

            Long affectedRows = (Long)res.get(0).get(0);

            assertNotNull("No rows was updated", affectedRows);
            assertTrue("affectedRows=" + affectedRows, affectedRows > 0);

            return affectedRows;
        }
        finally {
            U.closeQuiet(watchSvc);
        }
    }
}

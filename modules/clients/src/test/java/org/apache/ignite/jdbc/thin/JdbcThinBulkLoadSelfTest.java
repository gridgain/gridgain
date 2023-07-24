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

package org.apache.ignite.jdbc.thin;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.ComparisonFailure;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.Arrays;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.junit.Assume.assumeTrue;

/**
 * COPY statement tests.
 */
@RunWith(Parameterized.class)
public class JdbcThinBulkLoadSelfTest extends JdbcAbstractBulkLoadSelfTest {

    /** Basic COPY statement used in majority of the tests. */
    public static final String BASIC_SQL_COPY_STMT =
        "copy from '" + BULKLOAD_TWO_LINES_CSV_FILE + "'" +
            " into " + TBL_NAME +
            " (_key, age, firstName, lastName)" +
            " format csv";

    /** Parametrized run param : cacheMode. */
    @Parameterized.Parameter(0)
    public CacheMode cacheMode = REPLICATED;

    /** Parametrized run param : atomicity. */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode atomicityMode = ATOMIC;

    /** Parametrized run param : near mode. */
    @Parameterized.Parameter(2)
    public Boolean isNear = false;

    @Parameterized.Parameter(3)
    public Boolean isLegacyCopyEnabledParam;

    /** Test run configurations: Cache mode, atomicity type, is near. */
    @Parameterized.Parameters(name = "cacheMode={0}, atomicityMode={1}, isNear={2}, isLegacyCopyEnabledParam={3}")
    public static Collection<Object[]> runConfig() {
        return Arrays.asList(new Object[][] {
            {PARTITIONED, ATOMIC, true, true},
                {PARTITIONED, ATOMIC, true, false},
            {PARTITIONED, ATOMIC, false, true},
                {PARTITIONED, ATOMIC, false, false},
            {PARTITIONED, TRANSACTIONAL, true, true},
                {PARTITIONED, TRANSACTIONAL, true, false},
            {PARTITIONED, TRANSACTIONAL, false, true},
                {PARTITIONED, TRANSACTIONAL, false, false},
            {REPLICATED, ATOMIC, false, true},
                {REPLICATED, ATOMIC, false, false},
            {REPLICATED, TRANSACTIONAL, false, true},
                {REPLICATED, TRANSACTIONAL, false, false},
        });
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfig() {
        return cacheConfigWithIndexedTypes();
    }

    /**
     * Creates cache configuration with {@link QueryEntity} created
     * using {@link CacheConfiguration#setIndexedTypes(Class[])} call.
     *
     * @return The cache configuration.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheConfigWithIndexedTypes() {
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(cacheMode);
        cache.setAtomicityMode(atomicityMode);
        cache.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode == PARTITIONED)
            cache.setBackups(1);

        if (isNear)
            cache.setNearConfiguration(new NearCacheConfiguration());

        cache.setIndexedTypes(
            String.class, Person.class
        );

        return cache;
    }

    /**
     * Creates cache configuration with {@link QueryEntity} created
     * using {@link CacheConfiguration#setQueryEntities(Collection)} call.
     *
     * @return The cache configuration.
     */
    private CacheConfiguration cacheConfigWithQueryEntity() {
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);

        QueryEntity e = new QueryEntity();

        e.setKeyType(String.class.getName());
        e.setValueType("Person");

        e.addQueryField("id", Integer.class.getName(), null);
        e.addQueryField("age", Integer.class.getName(), null);
        e.addQueryField("firstName", String.class.getName(), null);
        e.addQueryField("lastName", String.class.getName(), null);

        cache.setQueryEntities(Collections.singletonList(e));

        return cache;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stmt = conn.createStatement();

        this.isLegacyCopyEnabled = isLegacyCopyEnabledParam;

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed())
            stmt.close();

        assertTrue(stmt.isClosed());

        super.afterTest();
    }

    @Override
    protected Connection createConnection() throws SQLException {
        if(this.isLegacyCopyEnabledParam) {
            return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?isLegacyCopyEnabled=true");
        } else {
            return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
        }
    }


    /**
     * Imports three-entry CSV file into a table and checks the entry created using SELECT statement with null string
     * specified and trim OFF.
     * This test verifies that the field which is equal to nullstring after trimming whitespaces will fail on insert
     * with trim turned off with 'Value conversion failed' message.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testThreeLineFileWithEmptyNumericColumnWithNullStringAndTrimOff() throws SQLException {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from '" + BULKLOAD_WITH_NULL_STRING + "' into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format csv nullstring 'a' trim off");

                return null;
            }
        }, SQLException.class, "Value conversion failed");
    }

    /**
     * Imports three-entry CSV file into a table and checks the entry created using SELECT statement with null string
     * specified and trim ON.
     * This test verifies that the field which is equal to nullstring after trimming whitespaces will be correctly
     * interpreted as null and will result in integer default value (0).
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testThreeLineFileWithEmptyNumericColumnWithNullStringAndTrimOn() throws SQLException {
        assumeTrue(isLegacyCopyEnabled);
        int updatesCnt = stmt.executeUpdate(
            "copy from '" + BULKLOAD_WITH_NULL_STRING + "' into " + TBL_NAME +
                " (_key, age, firstName, lastName)" +
                " format csv nullstring 'a' trim on");

        assertEquals(3, updatesCnt);

        checkCacheContents(TBL_NAME, true, 3);
    }

    /**
     * Imports three-entry CSV file into a table and checks the entry created using SELECT statement with trim OFF.
     * This test verifies that values will be inserted, but value conversion will fail on whitespace in the field ([ ]).
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testThreeLineFileWithEmptyNumericColumnWithTrimOn() throws SQLException {
        assumeTrue(isLegacyCopyEnabled);
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                int updatesCnt = stmt.executeUpdate(
                    "copy from '" + BULKLOAD_WITH_TRIM_OFF + "' into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format csv nullstring 'a' trim on");

                assertEquals(3, updatesCnt);

                checkCacheContents(TBL_NAME, true, 3);

                return null;
            }
        }, ComparisonFailure.class, "expected:<[ ]FirstName104");
    }

    /**
     * Verifies exception thrown if COPY is added into a packet.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testMultipleStatement() throws SQLException {
        assumeTrue(isLegacyCopyEnabled);
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.addBatch(BASIC_SQL_COPY_STMT);

                stmt.addBatch("copy from '" + BULKLOAD_ONE_LINE_CSV_FILE + "' into " + TBL_NAME +
                    " (_key, age, firstName, lastName)" +
                    " format csv");

                stmt.addBatch("copy from '" + BULKLOAD_UTF8_CSV_FILE + "' into " + TBL_NAME +
                    " (_key, age, firstName, lastName)" +
                    " format csv");

                stmt.executeBatch();

                return null;
            }
        }, BatchUpdateException.class, "COPY command cannot be executed in batch mode.");
    }

    /**
     * Test imports CSV file into a table on not affinity node and checks the created entries using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testBulkLoadToNonAffinityNode() throws Exception {
        IgniteEx client = startGrid(getConfiguration("client").setClientMode(true));

        try (Connection con = connect(client, null)) {
            con.setSchema('"' + DEFAULT_CACHE_NAME + '"');

            try (Statement stmt = con.createStatement()) {
                int updatesCnt = stmt.executeUpdate(
                        "copy from '" + BULKLOAD_UTF8_CSV_FILE + "' into " + TBL_NAME +
                                " (_key, age, firstName, lastName)" +
                                " format csv");

                assertEquals(2, updatesCnt);

                checkNationalCacheContents(TBL_NAME);
            }
        }

        stopGrid(client.name());
    }

    /**
     * Checks that bulk load works when we create table with {@link CacheConfiguration#setQueryEntities(Collection)}.
     * <p>
     * The majority of the tests in this class use {@link CacheConfiguration#setIndexedTypes(Class[])}
     * to create a table.
     *
     * @throws SQLException If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testConfigureQueryEntityAndBulkLoad() throws SQLException {
        ignite(0).getOrCreateCache(cacheConfigWithQueryEntity());

        int updatesCnt = stmt.executeUpdate(BASIC_SQL_COPY_STMT);

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Checks cache contents after bulk loading data in the above tests: ASCII version.
     * <p>
     * Uses SQL SELECT command for querying entries.
     *
     * @param tblName Table name to query.
     * @param checkLastName Check 'lastName' column (not imported in some tests).
     * @param recCnt Number of records to expect.
     * @throws SQLException When one of checks has failed.
     */
    private void checkCacheContents(String tblName, boolean checkLastName, int recCnt) throws SQLException {
        checkCacheContents(tblName, checkLastName, recCnt, ',');
    }

    /**
     * Checks cache contents after bulk loading data in the above tests: ASCII version.
     * <p>
     * Uses SQL SELECT command for querying entries.
     *
     * @param tblName Table name to query.
     * @param checkLastName Check 'lastName' column (not imported in some tests).
     * @param recCnt Number of records to expect.
     * @param delimiter The delimiter of fields.
     * @throws SQLException When one of checks has failed.
     */
    private void checkCacheContents(String tblName, boolean checkLastName, int recCnt, char delimiter) throws SQLException {
        ResultSet rs = stmt.executeQuery("select _key, age, firstName, lastName from " + tblName);

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("_key");

            SyntheticPerson sp = new SyntheticPerson(rs.getInt("age"),
                rs.getString("firstName"), rs.getString("lastName"));

            if (id == 101)
                sp.validateValues(0, "FirstName101 MiddleName101", "LastName101", checkLastName);
            else if (id == 102)
                sp.validateValues(0, "FirstName102 MiddleName102", "LastName102", checkLastName);
            else if (id == 103)
                sp.validateValues(0, "FirstName103 MiddleName103", "LastName103", checkLastName);
            else if (id == 104)
                sp.validateValues(0, " FirstName104 MiddleName104", "LastName104", checkLastName);
            else if (id == 123)
                sp.validateValues(12, "FirstName123 MiddleName123", "LastName123", checkLastName);
            else if (id == 234)
                sp.validateValues(23, "FirstName|234", null, checkLastName);
            else if (id == 345)
                sp.validateValues(34, "FirstName,345", null, checkLastName);
            else if (id == 456)
                sp.validateValues(45, "FirstName456", "LastName456", checkLastName);
            else if (id == 567)
                sp.validateValues(56, null, null, checkLastName);
            else if (id == 678)
                sp.validateValues(67, null, null, checkLastName);
            else if (id == 789)
                sp.validateValues(78, "FirstName789 plus \"quoted\"", "LastName 789", checkLastName);
            else if (id == 101112)
                sp.validateValues(1011, "FirstName 101112",
                    "LastName\"" + delimiter + "\" 1011" + delimiter + " 12", checkLastName);
            else
                fail("Wrong ID: " + id);

            cnt++;
        }

        assertEquals(recCnt, cnt);
    }

    /**
     *
     */
    protected class SyntheticPerson {
        /** */
        int age;

        /** */
        String firstName;

        /** */
        String lastName;

        /** */
        public SyntheticPerson(int age, String firstName, String lastName) {
            this.age = age;
            this.firstName = firstName;
            this.lastName = lastName;
        }

        /** */
        public void validateValues(int age, String firstName, String lastName, boolean checkLastName) {
            assertEquals(age, this.age);
            assertEquals(firstName, this.firstName);
            if (checkLastName)
                assertEquals(lastName, this.lastName);
        }
    }
}

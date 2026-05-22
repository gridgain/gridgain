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

package org.apache.ignite.cache.store.jdbc;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.jdbc.dialect.MariaDBDialect;
import org.apache.ignite.cache.store.jdbc.dialect.MySQLDialect;
import org.apache.ignite.cache.store.jdbc.model.Gender;
import org.apache.ignite.cache.store.jdbc.model.Person;
import org.apache.ignite.cache.store.jdbc.model.PersonKey;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.testcontainers.mariadb.MariaDBContainer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Integration tests for {@link MariaDBDialect} executed against a real MariaDB instance
 * managed by Testcontainers. Verifies that {@code CacheJdbcPojoStore} configured with
 * {@link MariaDBDialect} drives correct CRUD, MERGE-via-{@code ON DUPLICATE KEY UPDATE},
 * batched bulk operations, identifier escaping, transactional semantics, and parallel
 * {@code loadCache} behaviour against the live database.
 */
public class MariaDBCacheStoreTest extends GridCommonAbstractTest {
    /**
     * MariaDB container — started once for the whole test class.
     * MariaDB image pinned to current LTS — supports {@code INSERT ... ON DUPLICATE KEY UPDATE}.
     */
    private static MariaDBContainer container = new MariaDBContainer("mariadb:11.8");

    /**
     * Cache name used by all tests that rely on the default {@code Person} mapping.
     */
    private static final String CACHE_NAME = "test-cache";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        container.start();

        try (Connection conn = container.createConnection("");
             Statement stmt = conn.createStatement()
        ) {
            stmt.executeUpdate("CREATE TABLE Person (id INT PRIMARY KEY, org_id INT, birthday DATE, name VARCHAR(50), gender VARCHAR(50))");

            // Reserved-word table + column name; exercises backtick escaping when sqlEscapeAll=true.
            stmt.executeUpdate("CREATE TABLE `Person ORDER BY` (id INT PRIMARY KEY, org_id INT, birthday DATE, `name GROUP BY` VARCHAR(50), gender VARCHAR(50))");

            // MariaDB-specific data-type fixtures (Tier 3): microsecond TIMESTAMP and high-precision DECIMAL.
            stmt.executeUpdate("CREATE TABLE TypeSample (id INT PRIMARY KEY, ts TIMESTAMP(6), amount DECIMAL(38,10))");
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        try {
            if (container != null) {
                container.stop();
                container = null;
            }
        } finally {
            super.afterTestsStopped();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        try (Connection conn = container.createConnection("");
             Statement stmt = conn.createStatement()
        ) {
            stmt.executeUpdate("DELETE FROM Person");
            stmt.executeUpdate("DELETE FROM `Person ORDER BY`");
            stmt.executeUpdate("DELETE FROM TypeSample");
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            stopAllGrids();
        } finally {
            super.afterTest();
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<PersonKey, Person> cc = cacheConfiguration(personJdbcType(CACHE_NAME, "Person"), CacheAtomicityMode.ATOMIC, false);

        cfg.setCacheConfiguration(cc);

        return cfg;
    }

    /**
     * Maps the {@link Person} POJO to the given table. {@code salary} is omitted intentionally.
     *
     * @param cacheName Cache name to bind this type mapping to.
     * @param dbTable Database table name.
     */
    private static JdbcType[] personJdbcType(String cacheName, String dbTable) {
        JdbcType type = new JdbcType();

        type.setCacheName(cacheName);
        type.setDatabaseTable(dbTable);
        type.setKeyType(PersonKey.class.getName());
        type.setKeyFields(new JdbcTypeField(Types.INTEGER, "id", Integer.class, "id"));
        type.setValueType(Person.class.getName());
        type.setValueFields(
                new JdbcTypeField(Types.INTEGER, "id", Integer.class, "id"),
                new JdbcTypeField(Types.INTEGER, "org_id", Integer.class, "orgId"),
                new JdbcTypeField(Types.DATE, "birthday", Date.class, "birthday"),
                new JdbcTypeField(Types.VARCHAR, "name", String.class, "name"),
                new JdbcTypeField(Types.VARCHAR, "gender", Gender.class, "gender"));

        return new JdbcType[]{type};
    }

    /**
     * Regression guard: in MariaDB Connector/J, do not use {@link Integer#MIN_VALUE} for a streaming result set.
     * Before version 1.4.0, the only accepted value for fetch size was
     * {@code Statement.setFetchSize(Integer.MIN_VALUE)} (equivalent to {@code Statement.setFetchSize(1)}).
     * This value is still accepted for compatibility reasons, but rather use {@code Statement.setFetchSize(1)},
     * since according to JDBC the value must be >= 0.
     *
     * @see <a href="https://mariadb.com/docs/connectors/mariadb-connector-j/about-mariadb-connector-j#streaming-result-sets">MariaDB
     * Connector/J streaming result sets</a>
     */
    @Test
    public void testFetchSizeIsOne() {
        assertEquals(1, new MariaDBDialect().getFetchSize());
    }

    /**
     * Round-trips a single entry via the cache and verifies it landed in MariaDB.
     */
    @Test
    public void testWriteReadDelete() throws Exception {
        startGrid();

        IgniteCache<PersonKey, Person> cache = grid().cache(CACHE_NAME);

        PersonKey key = new PersonKey(1);
        Person val = new Person(1, 100, Date.valueOf("1990-01-01"), "Alice", null, Gender.FEMALE);

        cache.put(key, val);

        executeQueryInContainer("SELECT name, gender FROM Person WHERE id = 1", rs -> {
            assertTrue("Expected row in MariaDB after cache.put", rs.next());
            assertEquals(val.getName(), rs.getString(1));
            assertEquals(val.getGender().ordinal(), rs.getInt(2));
        });

        Person fromCache = cache.get(key);

        assertNotNull("cache.get returned null after put", fromCache);
        assertEquals(val.getName(), fromCache.getName());
        assertEquals(val.getGender(), fromCache.getGender());

        cache.remove(key);

        executeQueryInContainer("SELECT COUNT(*) FROM Person WHERE id = 1", rs -> {
            assertTrue(rs.next());
            assertEquals("Row should be deleted from MariaDB", 0, rs.getInt(1));
        });
    }

    /**
     * Re-puts the same keys with new values via {@code putAll}; the second batch must update
     * rather than fail with a duplicate-key error, proving the {@code ON DUPLICATE KEY UPDATE}
     * MERGE path runs.
     */
    @Test
    public void testWriteAllUsesMerge() throws Exception {
        startGrid();

        IgniteCache<PersonKey, Person> cache = grid().cache(CACHE_NAME);

        Map<PersonKey, Person> first = new LinkedHashMap<>();
        Map<PersonKey, Person> second = new LinkedHashMap<>();

        for (int i = 0; i < 5; i++) {
            first.put(new PersonKey(i),
                    new Person(i, 0, Date.valueOf("1990-01-01"), "v1-" + i, null, Gender.MALE));
            second.put(new PersonKey(i),
                    new Person(i, 0, Date.valueOf("1990-01-01"), "v2-" + i, null, Gender.FEMALE));
        }

        cache.putAll(first);
        cache.putAll(second);

        executeQueryInContainer("SELECT id, name FROM Person ORDER BY id", rs -> {
            int cnt = 0;

            while (rs.next()) {
                int id = rs.getInt(1);

                assertEquals("Second putAll should have overwritten id=" + id,
                        "v2-" + id, rs.getString(2));

                cnt++;
            }

            assertEquals(5, cnt);
        });
    }

    /**
     * Pre-seeds enough rows directly in MariaDB to trigger the parallel-load path,
     * then asserts {@code cache.loadCache(null)} pulls them all in.
     */
    @Test
    public void testLoadCacheParallel() throws Exception {
        // Exceed ParallelLoadCacheMinimumThreshold intentionally.
        int loadCacheRows = 600;

        insertPersonRows(loadCacheRows);

        startGrid();

        IgniteCache<PersonKey, Person> cache = grid().cache(CACHE_NAME);

        cache.loadCache(null);

        assertEquals(loadCacheRows, cache.size());

        for (int i = 0; i < loadCacheRows; i += 100) {
            Person p = cache.get(new PersonKey(i));

            assertNotNull("Missing key " + i + " after loadCache", p);
            assertEquals("name" + i, p.getName());
        }
    }

    /**
     * With {@code sqlEscapeAll=true}, every identifier emitted by the store passes through
     * {@link MySQLDialect#escape(String)} — which
     * wraps it in backticks. Using a MariaDB reserved word as both the table name and a column
     * name (table {@code `Person ORDER BY`}, column {@code `name GROUP BY`}) means the dialect MUST escape
     * correctly or the server returns a {@code SQLSyntaxErrorException}.
     */
    @Test
    public void testSqlEscapeAllWithBackticks() throws Exception {
        startGrid();

        // Reserved-word table + column name;
        JdbcType[] types = personJdbcType("cache-sql-escape-reserved", "Person ORDER BY");
        Arrays.stream(types[0].getValueFields())
                .filter(f -> "name".equals(f.getDatabaseFieldName()))
                .forEach(f -> f.setDatabaseFieldName("name GROUP BY"));

        CacheConfiguration<PersonKey, Person> cc = cacheConfiguration(types, CacheAtomicityMode.ATOMIC, true);

        IgniteCache<PersonKey, Person> cache = grid().createCache(cc);

        PersonKey key = new PersonKey(1);
        Person val = new Person(1, 100, Date.valueOf("1990-01-01"), "Alice", null, Gender.FEMALE);

        cache.put(key, val);

        Person fromCache = cache.get(key);

        assertNotNull("cache.get returned null after put on escape-cache", fromCache);
        assertEquals("Alice", fromCache.getName());

        executeQueryInContainer("SELECT `name GROUP BY` FROM `Person ORDER BY` WHERE id = 1", rs -> {
            assertTrue("Expected row in `Person ORDER BY` after cache.put", rs.next());
            assertEquals("Alice", rs.getString(1));
        });

        cache.remove(key);

        executeQueryInContainer("SELECT COUNT(*) FROM `Person ORDER BY`", rs -> {
            assertTrue(rs.next());
            assertEquals("Row should be deleted from `Person ORDER BY`", 0, rs.getInt(1));
        });
    }

    /**
     * Pre-seeds N rows directly in MariaDB, then drives bulk read-through via
     * {@code cache.getAll(keys)}. Exercises {@code BasicJdbcDialect.loadQuery}, which emits
     * {@code SELECT ... WHERE id IN (?,?,...)} for single-column keys, against real Connector/J.
     */
    @Test
    public void testLoadAllReadThrough() throws Exception {
        int n = 25;

        insertPersonRows(n);

        startGrid();

        IgniteCache<PersonKey, Person> cache = grid().cache(CACHE_NAME);

        Set<PersonKey> keys = new HashSet<>();

        for (int i = 0; i < n; i++) {
            keys.add(new PersonKey(i));
        }

        Map<PersonKey, Person> loaded = cache.getAll(keys);

        assertEquals(n, loaded.size());

        for (int i = 0; i < n; i++) {
            Person p = loaded.get(new PersonKey(i));

            assertNotNull("getAll missing key " + i, p);
            assertEquals("name" + i, p.getName());
        }
    }

    /**
     * Puts {@code N} entries via {@code putAll}, then drives bulk delete via
     * {@code cache.removeAll(keys)}. Each delete is a single-key {@code DELETE} prepared statement
     * batched through JDBC {@code addBatch/executeBatch}; this asserts the batched-delete path
     * works end-to-end against MariaDB.
     */
    @Test
    public void testRemoveAllBatched() throws Exception {
        startGrid();

        IgniteCache<PersonKey, Person> cache = grid().cache(CACHE_NAME);

        int n = 50;

        Map<PersonKey, Person> entries = new HashMap<>();

        for (int i = 0; i < n; i++) {
            entries.put(new PersonKey(i),
                    new Person(i, 0, Date.valueOf("1990-01-01"), "name" + i, null, Gender.MALE));
        }

        cache.putAll(entries);

        Set<PersonKey> keys = new HashSet<>(entries.keySet());

        cache.removeAll(keys);

        executeQueryInContainer("SELECT COUNT(*) FROM Person", rs -> {
            assertTrue(rs.next());
            assertEquals("All rows should be deleted from MariaDB", 0, rs.getInt(1));
        });
    }

    /**
     * Writes a {@link Person} with NULL {@code birthday} and NULL {@code name}, asserts both
     * round-trip as Java {@code null} and that the underlying row in MariaDB stores SQL NULL
     * (verified via {@link ResultSet#wasNull()}). Exercises Connector/J's null handling on
     * {@code setNull(Types.DATE, …)} / {@code getDate} / {@code getString}.
     */
    @Test
    public void testNullableColumnRoundTrip() throws Exception {
        startGrid();

        IgniteCache<PersonKey, Person> cache = grid().cache(CACHE_NAME);

        PersonKey key = new PersonKey(42);
        Person val = new Person(42, 7, null, null, null, Gender.MALE);

        cache.put(key, val);

        executeQueryInContainer("SELECT birthday, name FROM Person WHERE id = 42", rs -> {
            assertTrue("Expected row in MariaDB after cache.put", rs.next());

            rs.getDate(1);
            assertTrue("birthday should be SQL NULL", rs.wasNull());

            rs.getString(2);
            assertTrue("name should be SQL NULL", rs.wasNull());
        });

        // Drop the cache entry so cache.get drives read-through and exercises the SELECT-with-nulls path.
        cache.clear();

        Person fromCache = cache.get(key);

        assertNotNull("cache.get should not return null when row exists in DB", fromCache);
        assertNull("birthday should round-trip as null", fromCache.getBirthday());
        assertNull("name should round-trip as null", fromCache.getName());
    }

    /**
     * With {@code batchSize=10} and 25 entries, {@code putAll} must split into three JDBC batches.
     * Verifies that re-preparation across batches keeps MERGE semantics intact: a second
     * {@code putAll} with overwritten values updates every row, not just the rows in the final
     * batch.
     */
    @Test
    public void testWriteAllSplitsAcrossBatches() throws Exception {
        startGrid();

        CacheConfiguration<PersonKey, Person> cc = cacheConfiguration(personJdbcType("cache-small-batch", "Person"), CacheAtomicityMode.ATOMIC, false);

        ((MariaDBCacheStoreFactory) cc.getCacheStoreFactory()).setBatchSize(10);

        IgniteCache<PersonKey, Person> cache = grid().createCache(cc);

        int n = 25;

        Map<PersonKey, Person> first = new HashMap<>();
        Map<PersonKey, Person> second = new HashMap<>();

        for (int i = 0; i < n; i++) {
            first.put(new PersonKey(i),
                    new Person(i, 0, Date.valueOf("1990-01-01"), "v1-" + i, null, Gender.MALE));
            second.put(new PersonKey(i),
                    new Person(i, 0, Date.valueOf("1990-01-01"), "v2-" + i, null, Gender.FEMALE));
        }

        cache.putAll(first);

        executeQueryInContainer("SELECT COUNT(*) FROM Person", rs -> {
            assertTrue(rs.next());
            assertEquals("All " + n + " rows should be inserted across batches", n, rs.getInt(1));
        });

        cache.putAll(second);

        executeQueryInContainer("SELECT id, name FROM Person ORDER BY id", rs -> {
            int cnt = 0;

            while (rs.next()) {
                int id = rs.getInt(1);

                assertEquals("Every row should be overwritten by the second putAll across batches",
                        "v2-" + id, rs.getString(2));

                cnt++;
            }

            assertEquals(n, cnt);
        });
    }

    /**
     * In an explicit transaction with {@code TRANSACTIONAL} atomicity, a successful
     * {@code tx.commit()} must drive {@code sessionEnd(true)} → {@code conn.commit()} on the
     * underlying JDBC connection and persist the row in InnoDB.
     */
    @Test
    public void testTransactionalCommit() throws Exception {
        Ignite ignite = startGrid();

        CacheConfiguration<PersonKey, Person> cc = cacheConfiguration(personJdbcType("tx-commit-cache", "Person"), CacheAtomicityMode.TRANSACTIONAL, false);
        IgniteCache<PersonKey, Person> cache = ignite.createCache(cc);

        PersonKey key = new PersonKey(1);
        Person val = new Person(1, 10, Date.valueOf("1990-01-01"), "Tx-Alice", null, Gender.FEMALE);

        try (Transaction tx = ignite.transactions().txStart()) {
            cache.put(key, val);

            tx.commit();
        }

        executeQueryInContainer("SELECT name FROM Person WHERE id = 1", rs -> {
            assertTrue("Row should be visible in MariaDB after commit", rs.next());
            assertEquals("Tx-Alice", rs.getString(1));
        });
    }

    /**
     * In an explicit transaction with {@code TRANSACTIONAL} atomicity, an implicit rollback
     * (no {@code tx.commit()}) must drive {@code sessionEnd(false)} → {@code conn.rollback()}
     * on the underlying JDBC connection. InnoDB must NOT have the row.
     */
    @Test
    public void testTransactionalRollback() throws Exception {
        Ignite ignite = startGrid();

        CacheConfiguration<PersonKey, Person> cc = cacheConfiguration(personJdbcType("tx-rollback-cache", "Person"), CacheAtomicityMode.TRANSACTIONAL, false);
        IgniteCache<PersonKey, Person> cache = ignite.createCache(cc);

        PersonKey key = new PersonKey(2);
        Person val = new Person(2, 10, Date.valueOf("1990-01-01"), "Tx-Bob", null, Gender.MALE);

        try (Transaction tx = ignite.transactions().txStart()) {
            cache.put(key, val);

            tx.rollback();
        }

        executeQueryInContainer("SELECT COUNT(*) FROM Person WHERE id = 2", rs -> {
            assertTrue(rs.next());
            assertEquals("Row must not be present in MariaDB after rollback", 0, rs.getInt(1));
        });

        assertNull("Cache must not hold the value after rollback", cache.get(key));
    }

    /**
     * Same as {@link #testLoadCacheParallel} but the row count is intentionally not a clean
     * multiple of the parallel-load threshold. Guards against off-by-one errors in the
     * dialect's {@code @rownum mod ?} partitioning.
     */
    @Test
    public void testLoadCacheParallelOddRowCount() throws Exception {
        // Exceed ParallelLoadCacheMinimumThreshold intentionally.
        int oddCnt = 555;

        insertPersonRows(oddCnt);

        startGrid();

        IgniteCache<PersonKey, Person> cache = grid().cache(CACHE_NAME);

        cache.loadCache(null);

        assertEquals(oddCnt, cache.size());

        // Spot-check the boundary rows.
        for (int i : new int[]{0, oddCnt / 2, oddCnt - 1}) {
            Person p = cache.get(new PersonKey(i));

            assertNotNull("Missing key " + i + " after loadCache (odd row count)", p);
            assertEquals("name" + i, p.getName());
        }
    }

    /**
     * {@code cache.get} for a key not present in MariaDB must drive a single-key SELECT that
     * returns an empty result set, and yield {@code null} — not throw.
     */
    @Test
    public void testLoadFromMissingKey() throws Exception {
        startGrid();

        IgniteCache<PersonKey, Person> cache = grid().cache(CACHE_NAME);

        assertNull("cache.get for absent key must return null", cache.get(new PersonKey(999)));
    }

    /**
     * Exercises two MariaDB data types whose precision/scale differ meaningfully from H2:
     * {@code TIMESTAMP(6)} (microsecond precision) and {@code DECIMAL(38,10)} (high precision).
     * Catches transformer or driver-level truncation/rounding regressions when the store
     * round-trip values through {@code java.sql.Timestamp} and {@code BigDecimal}.
     */
    @Test
    public void testMariaDBSpecificDataTypes() throws Exception {
        Ignite ignite = startGrid();

        CacheConfiguration<Integer, TypeSample> cc = cacheConfiguration(typeSampleJdbcType("cache-types"), CacheAtomicityMode.ATOMIC, false);

        IgniteCache<Integer, TypeSample> cache = ignite.createCache(cc);

        Timestamp microTs = Timestamp.valueOf("2025-01-15 12:34:56.123456");
        BigDecimal bigAmount = new BigDecimal("12345678901234567890.1234567890");

        TypeSample val = new TypeSample(1, microTs, bigAmount);

        cache.put(1, val);

        TypeSample fromCache = cache.get(1);

        assertNotNull(fromCache);
        assertEquals("TIMESTAMP(6) must preserve microsecond precision", microTs, fromCache.getTs());
        assertEquals("DECIMAL(38,10) must preserve precision", bigAmount, fromCache.getAmount());

        // Cross-check the raw row in MariaDB to confirm storage, not just cache round-trip.
        executeQueryInContainer("SELECT ts, amount FROM TypeSample WHERE id = 1", rs -> {
            assertTrue(rs.next());
            assertEquals(microTs, rs.getTimestamp(1));
            assertEquals(bigAmount, rs.getBigDecimal(2));
        });
    }

    /**
     * Starts two nodes, writes through the cache, stops everything, then restarts a single
     * fresh node and reads via read-through. Asserts the JDBC store survives node lifecycle
     * end-to-end against MariaDB and that data written by one topology is recoverable by
     * another.
     */
    @Test
    public void testMultinodeRestart() throws Exception {
        startGrids(2);

        IgniteCache<PersonKey, Person> cache = grid(0).cache(CACHE_NAME);

        int n = 20;

        Map<PersonKey, Person> entries = new HashMap<>();

        for (int i = 0; i < n; i++) {
            entries.put(new PersonKey(i),
                    new Person(i, 100, Date.valueOf("1990-01-01"), "name" + i, null, Gender.MALE));
        }

        cache.putAll(entries);

        executeQueryInContainer("SELECT COUNT(*) FROM Person", rs -> {
            assertTrue(rs.next());
            assertEquals("All entries should land in MariaDB before restart", n, rs.getInt(1));
        });

        stopAllGrids();

        startGrid();

        IgniteCache<PersonKey, Person> restartedCache = grid().cache(CACHE_NAME);

        for (int i = 0; i < n; i++) {
            Person p = restartedCache.get(new PersonKey(i));

            assertNotNull("Read-through failed for key " + i + " after restart", p);
            assertEquals("name" + i, p.getName());
        }
    }

    /**
     * Builds a cache configuration backed by the default {@code Person} MariaDB store.
     */
    private static <K, V> CacheConfiguration<K, V> cacheConfiguration(JdbcType[] types, CacheAtomicityMode mode, boolean sqlEscapeAll) {
        MariaDBCacheStoreFactory<K, V> sf = new MariaDBCacheStoreFactory<>();

        sf.setDataSourceFactory(container.getJdbcUrl(), container.getUsername(), container.getPassword());
        sf.setTypes(types);
        sf.setParallelLoadCacheMinimumThreshold(512);
        sf.setSqlEscapeAll(sqlEscapeAll);

        CacheConfiguration<K, V> cc = new CacheConfiguration<>(types[0].getCacheName());

        cc.setCacheMode(CacheMode.PARTITIONED);
        cc.setAtomicityMode(mode);
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);
        cc.setCacheStoreFactory(sf);

        return cc;
    }

    /**
     * JDBC type mapping for {@link TypeSample} backed by the {@code TypeSample} table.
     */
    private static JdbcType[] typeSampleJdbcType(String cacheName) {
        JdbcType type = new JdbcType();

        type.setCacheName(cacheName);
        type.setDatabaseTable("TypeSample");
        type.setKeyType(Integer.class.getName());
        type.setKeyFields(new JdbcTypeField(Types.INTEGER, "id", Integer.class, "id"));
        type.setValueType(TypeSample.class.getName());
        type.setValueFields(
                new JdbcTypeField(Types.INTEGER, "id", Integer.class, "id"),
                new JdbcTypeField(Types.TIMESTAMP, "ts", Timestamp.class, "ts"),
                new JdbcTypeField(Types.DECIMAL, "amount", BigDecimal.class, "amount"));

        return new JdbcType[]{type};
    }

    /**
     * Pre-seeds {@code n} rows directly into the {@code Person} table via a single JDBC batch.
     */
    private static void insertPersonRows(int n) throws Exception {
        try (Connection conn = container.createConnection("");
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO Person(id, org_id, birthday, name, gender) VALUES (?, ?, ?, ?, ?)")
        ) {
            for (int i = 0; i < n; i++) {
                ps.setInt(1, i);
                ps.setInt(2, i % 10);
                ps.setDate(3, Date.valueOf("1990-01-01"));
                ps.setString(4, "name" + i);
                ps.setString(5, Gender.MALE.toString());

                ps.addBatch();
            }

            ps.executeBatch();
        }
    }

    private static void executeQueryInContainer(String query, ThrowingConsumer<ResultSet> consumer) throws Exception {
        try (Connection conn = container.createConnection("");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)
        ) {
            consumer.accept(rs);
        }
    }

    interface ThrowingConsumer<T> {
        void accept(T t) throws Exception;
    }

    /**
     * Value class for {@link #testMariaDBSpecificDataTypes}. Kept inline to avoid adding a
     * top-level fixture for a single test.
     */
    public static class TypeSample implements Serializable {
        private static final long serialVersionUID = 0L;

        private Integer id;

        private Timestamp ts;

        private BigDecimal amount;

        public TypeSample() {
            // No-op.
        }

        public TypeSample(Integer id, Timestamp ts, BigDecimal amount) {
            this.id = id;
            this.ts = ts;
            this.amount = amount;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public Timestamp getTs() {
            return ts;
        }

        public void setTs(Timestamp ts) {
            this.ts = ts;
        }

        public BigDecimal getAmount() {
            return amount;
        }

        public void setAmount(BigDecimal amount) {
            this.amount = amount;
        }
    }
}

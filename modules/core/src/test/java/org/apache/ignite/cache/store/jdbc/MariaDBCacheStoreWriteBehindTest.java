/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.jdbc.model.Gender;
import org.apache.ignite.cache.store.jdbc.model.Person;
import org.apache.ignite.cache.store.jdbc.model.PersonKey;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.mariadb.MariaDBContainer;

import java.sql.Date;
import java.sql.Types;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.ignite.cache.store.jdbc.MariaDBImages.executeQuery;
import static org.apache.ignite.cache.store.jdbc.MariaDBImages.executeUpdate;

/**
 * Integration tests for the MariaDB cache store ({@link MariaDBCacheStore} via
 * {@link MariaDBCacheStoreFactory}) with <b>write-behind</b> enabled, executed against a real
 * MariaDB instance managed by Testcontainers.
 * <p>
 * Unlike {@link MariaDBCacheStoreTest} (write-through only), these tests exercise the
 * asynchronous {@code GridCacheWriteBehindStore} path: cache updates are buffered and flushed to
 * MariaDB later — by flush size, by flush frequency, coalesced per key, on graceful node stop,
 * in non-coalescing mode, and from within a transaction. Because flushing is asynchronous, each
 * assertion against the database is wrapped in {@link GridTestUtils#waitForCondition} rather than
 * checked inline.
 */
@RunWith(Parameterized.class)
public class MariaDBCacheStoreWriteBehindTest extends GridCommonAbstractTest {
    /**
     * MariaDB image under test, injected by {@link Parameterized}. One value per
     * {@link MariaDBImages#lts()} row, so every test runs once per MariaDB version.
     */
    @Parameterized.Parameter
    public String image;

    /**
     * MariaDB container for the {@link #image} currently under test. Started lazily and swapped in
     * {@link #beforeTest()} when {@link #image} changes;
     */
    private static MariaDBContainer container;

    /**
     * @return MariaDB image tags to run every test against (the LTS matrix).
     */
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> images() {
        return MariaDBImages.lts();
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

        containerStart();
    }

    /**
     * Starts the MariaDB {@link #container} for the {@link #image} currently under test is
     * running, (re)starting it when the parameter changes, so at most one container is alive at a time.
     */
    private void containerStart() throws Exception {
        if (container != null && image.equals(container.getDockerImageName())) {
            executeUpdate(container,
                    "DELETE FROM Person"
            );

            return;
        }

        if (container != null) {
            container.stop();
        }

        container = new MariaDBContainer(image);
        container.start();

        executeUpdate(container,
                "CREATE TABLE Person (id INT PRIMARY KEY, org_id INT, birthday DATE, name VARCHAR(50), gender VARCHAR(50))"
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            stopAllGrids();
        } finally {
            super.afterTest();
        }
    }

    /**
     * With fewer buffered entries than the flush size, only the periodic flush (every
     * {@code flushFrequency} ms) can drain the buffer to MariaDB.
     */
    @Test
    public void testFlushByFrequency() throws Exception {
        Ignite ignite = startGrid();

        CacheConfiguration<PersonKey, Person> cc = writeBehindCacheConfiguration(
                "wb-by-frequency", CacheAtomicityMode.ATOMIC, 1024, 500, true);

        IgniteCache<PersonKey, Person> cache = ignite.createCache(cc);

        int n = 5;

        putPersons(cache, n);

        waitForPersonCount(container, n, 10_000);

        for (int i = 0; i < n; i++)
            assertEquals("name" + i, personName(container, i));
    }

    /**
     * A small flush size makes the buffer overflow trigger the flush; entries land in MariaDB
     * driven by the size threshold rather than the timer.
     */
    @Test
    public void testFlushBySize() throws Exception {
        Ignite ignite = startGrid();

        // Long-but-finite frequency only mops up any stragglers; the size threshold drives the flush.
        CacheConfiguration<PersonKey, Person> cc = writeBehindCacheConfiguration(
                "wb-by-size", CacheAtomicityMode.ATOMIC, 10, 2_000, true);

        IgniteCache<PersonKey, Person> cache = ignite.createCache(cc);

        int n = 50;

        putPersons(cache, n);

        waitForPersonCount(container, n, 15_000);
    }

    /**
     * With coalescing on, rapid-fire updates to the same key collapse into a single store write,
     * so the final value is what must end up in MariaDB.
     */
    @Test
    public void testCoalescing() throws Exception {
        Ignite ignite = startGrid();

        CacheConfiguration<PersonKey, Person> cc = writeBehindCacheConfiguration(
                "wb-coalescing", CacheAtomicityMode.ATOMIC, 1024, 1_000, true);

        IgniteCache<PersonKey, Person> cache = ignite.createCache(cc);

        PersonKey key = new PersonKey(1);

        int updates = 10;

        for (int i = 0; i < updates; i++)
            cache.put(key, new Person(1, 0, Date.valueOf("1990-01-01"), "v" + i, null, Gender.MALE));

        String expName = "v" + (updates - 1);

        assertTrue("Final coalesced value did not reach MariaDB",
                GridTestUtils.waitForCondition(() -> expName.equals(personName(container, 1)), 10_000));

        assertEquals("Coalescing must leave exactly one row for the key", 1, personCount(container));
    }

    /**
     * Non-coalescing write-behind keeps every operation in a per-flusher queue; all distinct entries
     * must still be flushed to MariaDB.
     */
    @Test
    public void testNonCoalescing() throws Exception {
        Ignite ignite = startGrid();

        CacheConfiguration<PersonKey, Person> cc = writeBehindCacheConfiguration(
                "wb-non-coalescing", CacheAtomicityMode.ATOMIC, 10, 500, false);

        IgniteCache<PersonKey, Person> cache = ignite.createCache(cc);

        int n = 30;

        putPersons(cache, n);

        waitForPersonCount(container, n, 10_000);

        assertEquals("name0", personName(container, 0));
        assertEquals("name29", personName(container, 29));
    }

    /**
     * Deletes are buffered and flushed just like writes: after the rows are persisted, removing the
     * keys must eventually clear them from MariaDB.
     */
    @Test
    public void testDeleteFlush() throws Exception {
        Ignite ignite = startGrid();

        CacheConfiguration<PersonKey, Person> cc = writeBehindCacheConfiguration(
                "wb-delete", CacheAtomicityMode.ATOMIC, 20, 500, true);

        IgniteCache<PersonKey, Person> cache = ignite.createCache(cc);

        int n = 20;

        putPersons(cache, n);

        waitForPersonCount(container, n, 10_000);

        Set<PersonKey> keys = new HashSet<>();

        for (int i = 0; i < n; i++)
            keys.add(new PersonKey(i));

        cache.removeAll(keys);

        waitForPersonCount(container, 0, 10_000);
    }

    /**
     * In a {@code TRANSACTIONAL} cache, {@code tx.commit()} returns immediately while the store write
     * is performed asynchronously by write-behind; the rows must land in MariaDB shortly after.
     */
    @Test
    public void testTransactionalWriteBehind() throws Exception {
        Ignite ignite = startGrid();

        CacheConfiguration<PersonKey, Person> cc = writeBehindCacheConfiguration(
                "wb-tx", CacheAtomicityMode.TRANSACTIONAL, 10, 500, true);

        IgniteCache<PersonKey, Person> cache = ignite.createCache(cc);

        int n = 5;

        try (Transaction tx = ignite.transactions().txStart()) {
            for (int i = 0; i < n; i++)
                cache.put(new PersonKey(i), new Person(i, 0, Date.valueOf("1990-01-01"), "tx" + i, null, Gender.FEMALE));

            tx.commit();
        }

        waitForPersonCount(container, n, 10_000);

        for (int i = 0; i < n; i++)
            assertEquals("tx" + i, personName(container, i));
    }

    /**
     * Builds a cache configuration backed by the {@code Person} MariaDB store with write-behind
     * enabled and the given tuning parameters.
     *
     * @param cacheName Cache name (also bound into the JDBC type mapping).
     * @param mode Cache atomicity mode.
     * @param flushSize Write-behind flush size (max buffered entries before a size-driven flush).
     * @param flushFreq Write-behind flush frequency in milliseconds.
     * @param coalescing Whether to coalesce repeated updates to the same key.
     */
    private static CacheConfiguration<PersonKey, Person> writeBehindCacheConfiguration(
            String cacheName,
            CacheAtomicityMode mode,
            int flushSize,
            long flushFreq,
            boolean coalescing
    ) {
        JdbcType[] types = personJdbcType(cacheName, "Person");

        CacheConfiguration<PersonKey, Person> cc = new CacheConfiguration<>(cacheName);

        cc.setCacheMode(CacheMode.PARTITIONED);
        cc.setAtomicityMode(mode);
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);
        cc.setCacheStoreFactory(mariaDbCacheStoreFactory(types));

        cc.setWriteBehindEnabled(true);
        cc.setWriteBehindFlushSize(flushSize);
        cc.setWriteBehindFlushFrequency(flushFreq);
        cc.setWriteBehindCoalescing(coalescing);

        // assertion (flusherCriticalSize > batchSize) in constructor GridCacheWriteBehindStore.Flusher
        cc.setWriteBehindBatchSize(flushSize / 2);

        return cc;
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
     * Builds a {@link MariaDBCacheStoreFactory} pointed at the Testcontainers MariaDB instance.
     */
    private static MariaDBCacheStoreFactory<PersonKey, Person> mariaDbCacheStoreFactory(JdbcType[] types) {
        MariaDBCacheStoreFactory<PersonKey, Person> sf = new MariaDBCacheStoreFactory<>();

        sf.setDataSourceFactory(container.getJdbcUrl(), container.getUsername(), container.getPassword());
        sf.setTypes(types);
        sf.setParallelLoadCacheMinimumThreshold(512);

        return sf;
    }

    /**
     * Puts {@code n} {@link Person} entries (keys {@code 0..n-1}) via a single {@code putAll}.
     */
    private static void putPersons(IgniteCache<PersonKey, Person> cache, int n) {
        Map<PersonKey, Person> entries = new LinkedHashMap<>();

        for (int i = 0; i < n; i++)
            entries.put(new PersonKey(i), new Person(i, i % 10, Date.valueOf("1990-01-01"), "name" + i, null, Gender.MALE));

        cache.putAll(entries);
    }

    /**
     * Waits until the {@code Person} table holds exactly {@code expected} rows, or fails after
     * {@code timeoutMs}. Used to assert on the asynchronous write-behind flush.
     */
    private static void waitForPersonCount(MariaDBContainer container, int expected, long timeoutMs) throws Exception {
        assertTrue("Timed out waiting for " + expected + " rows in MariaDB (write-behind flush); actual=" + personCount(container),
                GridTestUtils.waitForCondition(() -> personCount(container) == expected, timeoutMs));
    }

    /**
     * @return Current number of rows in the {@code Person} table.
     */
    private static int personCount(MariaDBContainer container) {
        try {
            return executeQuery(container, "SELECT COUNT(*) FROM Person", rs -> {
                rs.next();
                return rs.getInt(1);
            });
        }
        catch (Exception e) {
            throw new IgniteException("Failed to count Person rows", e);
        }
    }

    /**
     * @param id Person id.
     * @return {@code name} column for the given id in MariaDB, or {@code null} if absent.
     */
    private static String personName(MariaDBContainer container, int id) {
        try {
            return executeQuery(container, "SELECT name FROM Person WHERE id = " + id,
                    rs -> rs.next() ? rs.getString(1) : null);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to read Person name", e);
        }
    }
}

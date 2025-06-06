/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class BinarySimpleNameRegistrationTest extends GridCommonAbstractTest {

    private static final String CACHE_NAME = "TWO_TEST_CACHE";

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        return applyBinaryConfig(cfg);
    }

    private static IgniteConfiguration applyBinaryConfig(IgniteConfiguration cfg) {
        cfg.setBinaryConfiguration(new BinaryConfiguration()
            .setCompactFooter(true)
            .setNameMapper(new BinaryBasicNameMapper().setSimpleName(true)));

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    private void insertData() throws Exception {
        IgniteEx ignite = startClientGrid(1);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE_NAME);

        SqlFieldsQuery queryCreate = new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS " + CACHE_NAME + " (" +
            "ID INTEGER NOT NULL, " +
            "NAME VARCHAR NOT NULL, " +
            "ADDRESS VARCHAR NOT NULL, " +
            "primary key (ID)) " +
            "WITH \"atomicity=transactional, " +
            "key_type=org.apache.ignite.internal.processors.cache.BinarySimpleNameRegistrationTest$KnTwoTestCacheKey, " +
            "value_type=org.apache.ignite.internal.processors.cache.BinarySimpleNameRegistrationTest$KnTwoTestCacheValue, " +
            "cache_name=" + CACHE_NAME + "\";");

        cache.query(queryCreate).getAll();

        SqlFieldsQuery queryDelete = new SqlFieldsQuery("DELETE FROM " + CACHE_NAME);
        cache.query(queryDelete).getAll();

        SqlFieldsQuery queryInsert = new SqlFieldsQuery("insert into " + CACHE_NAME + " values (101,'sanjiv','acharyya')");
        cache.query(queryInsert).getAll();

        SqlFieldsQuery queryAll = new SqlFieldsQuery("SELECT * FROM " + CACHE_NAME);

        List<List<?>> all = cache.query(queryAll).getAll();
        assertEquals("1 entries should be on the cache", 1, all.size());

        List<Cache.Entry<Object, Object>> all1 = cache.withKeepBinary().query(new ScanQuery<>()).getAll();
        assertEquals("1 entries should be on the cache", 1, all1.size());

        System.out.println("*** INSERTING FINISHED ***");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldReadPreviouslyInsertedDataThickClient() throws Exception {
        startGrid(0);

        insertData();

        IgniteEx ignite2 = startClientGrid(2);

        IgniteCache<Object, Object> clientCache = ignite2.cache(CACHE_NAME);

        clientCache.put(new KnTwoTestCacheKey(102), new KnTwoTestCacheValue("name", "address"));
        assertEquals("Cache size", 2, clientCache.size());

        List<Cache.Entry<Object, Object>> entries = clientCache.query(new ScanQuery<>()).getAll();
        assertEquals("Size must be", 2, entries.size());
        assertTrue("Found element inserted via SQL", entries.stream().anyMatch(e -> new KnTwoTestCacheKey(101).equals(e.getKey())));
        assertTrue("Found element inserted via Java", entries.stream().anyMatch(e -> new KnTwoTestCacheKey(102).equals(e.getKey())));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldReadPreviouslyInsertedDataThinClient() throws Exception {
        startGrid(0);

        insertData();

        ClientConfiguration cfg = new ClientConfiguration();
        cfg.setAddresses("127.0.0.1:10800");

        IgniteClient client = Ignition.startClient(cfg);

        ClientCache<Object, Object> clientCache = client.cache(CACHE_NAME);

        clientCache.put(new KnTwoTestCacheKey(102), new KnTwoTestCacheValue("name", "address"));
        assertEquals("Cache size", 2, clientCache.size());

        List<Cache.Entry<Object, Object>> entries = clientCache.query(new ScanQuery<>()).getAll();
        assertEquals("Size must be", 2, entries.size());
        assertTrue("Found element inserted via SQL", entries.stream().anyMatch(e -> new KnTwoTestCacheKey(101).equals(e.getKey())));
        assertTrue("Found element inserted via Java", entries.stream().anyMatch(e -> new KnTwoTestCacheKey(102).equals(e.getKey())));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldWriteDataAndNotCauseDuplicatedRegistration() throws Exception {
        startGrid(0);

        insertData();

        ClientConfiguration cfg = new ClientConfiguration();
        cfg.setAddresses("127.0.0.1:10800");

        IgniteClient client = Ignition.startClient(cfg);

        ClientCache<Object, Object> clientCache = client.cache(CACHE_NAME);

        clientCache.put(new KnTwoTestCacheKey(102), new KnTwoTestCacheValue("name", "address"));
    }

    public class KnTwoTestCacheKey implements Serializable {

        private Integer ID;

        public KnTwoTestCacheKey(int i) {
            this.ID = i;
        }

        public Integer getId() {
            return ID;
        }

        public void setId(Integer id) {
            this.ID = id;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KnTwoTestCacheKey that = (KnTwoTestCacheKey) o;
            return Objects.equals(ID, that.ID);
        }

        @Override public int hashCode() {
            return Objects.hash(ID);
        }

        @Override public String toString() {
            return "KnTwoTestCacheKey{" +
                "id=" + ID +
                '}';
        }
    }

    public class KnTwoTestCacheValue implements Serializable {

        private String NAME;

        private String ADDRESS;

        public String getNAME() {
            return NAME;
        }

        public void setNAME(String NAME) {
            this.NAME = NAME;
        }

        public String getADDRESS() {
            return ADDRESS;
        }

        public void setADDRESS(String ADDRESS) {
            this.ADDRESS = ADDRESS;
        }

        public KnTwoTestCacheValue() {}

        public KnTwoTestCacheValue(String name, String address) {
            this.NAME = name;
            this.ADDRESS = address;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KnTwoTestCacheValue that = (KnTwoTestCacheValue) o;
            return Objects.equals(NAME, that.NAME) && Objects.equals(ADDRESS, that.ADDRESS);
        }

        @Override public int hashCode() {
            return Objects.hash(NAME, ADDRESS);
        }

        @Override public String toString() {
            return "KnTwoTestCacheValue{" +
                "name='" + NAME + '\'' +
                ", address='" + ADDRESS + '\'' +
                '}';
        }
    }

}


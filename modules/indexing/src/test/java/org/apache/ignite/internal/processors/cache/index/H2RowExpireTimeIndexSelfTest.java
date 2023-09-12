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

package org.apache.ignite.internal.processors.cache.index;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.result.SearchRow;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;

/**
 * Since sql (unlike cache api) doesn't remove expired rows, we need to check that expired rows are filtered by the
 * cursor. Background expired rows cleanup is turned off.
 */
public class H2RowExpireTimeIndexSelfTest extends GridCommonAbstractTest {
    /** In so milliseconds since creation row can be treated as expired. */
    private static final long EXPIRE_IN_MS_FROM_CREATE = 100L;

    /** How many milliseconds we are going to wait til, cache data row become expired. */
    private static final long WAIT_MS_TIL_EXPIRED = EXPIRE_IN_MS_FROM_CREATE * 2L;

    /**
     * Start only one grid.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        startGrids(1);
    }

    /** */
    @Override protected void afterTestsStopped() throws Exception {
        G.stopAll(true);

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /**
     * Cleans up before test.
     */
    @Before
    public void dropTestCache() {
        grid(0).destroyCache("notEager");
    }

    /**
     * Create test cache. We use {@link CacheConfiguration#isEagerTtl()} == false to have expired rows in the cache for
     * sure.
     */
    private IgniteCache<Integer, Integer> createTestCache() {
        CacheConfiguration ccfg = defaultCacheConfiguration()
            .setEagerTtl(false)
            .setName("notEager")
            .setQueryEntities(Collections.singleton(
                new QueryEntity("java.lang.Integer", "java.lang.Integer")
                    .setKeyFieldName("id")
                    .setValueFieldName("val")
                    .addQueryField("id", Integer.class.getName(), null)
                    .addQueryField("val", Integer.class.getName(), null)
                    .setIndexes(Collections.singleton(new QueryIndex("val")))
                    .setTableName("Integer")));

        IgniteCache<Integer, Integer> cache = grid(0).createCache(ccfg);

        return cache;
    }

    /**
     * Put values into the table with expire policy. Inserted row become expired in {@link #EXPIRE_IN_MS_FROM_CREATE}
     * milliseconds.
     *
     * @param cache cache to put values in.
     * @param key key of the row.
     * @param val value of the row.
     */
    private void putExpiredSoon(IgniteCache cache, Integer key, Integer val) {
        CreatedExpiryPolicy expireSinceCreated = new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS,
            EXPIRE_IN_MS_FROM_CREATE));

        IgniteCache<Integer, Integer> expCache = cache.withExpiryPolicy(expireSinceCreated);

        expCache.put(key, val);
    }

    /**
     * Put values into the table with expire policy.
     *
     * @param cache cache to put values in.
     * @param key key of the row.
     * @param val value of the row.
     */
    private void putExpireInYear(IgniteCache cache, Integer key, Integer val) {
        CreatedExpiryPolicy expireSinceCreated = new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS,
            TimeUnit.DAYS.toMillis(365)));

        IgniteCache<Integer, Integer> expCache = cache.withExpiryPolicy(expireSinceCreated);

        expCache.put(key, val);
    }

    /**
     * Expired row check of the tree index in case {@link H2TreeIndex#find(Session, SearchRow, SearchRow)} optimizes
     * returned cursor as SingleRowCursor.
     */
    @Test
    public void testTreeIndexSingleRow() throws Exception {
        IgniteCache<Integer, Integer> cache = createTestCache();

        cache.put(1, 2);
        cache.put(3, 4);

        putExpireInYear(cache, 5, 6);

        putExpiredSoon(cache, 42, 43);

        U.sleep(WAIT_MS_TIL_EXPIRED);

        {
            List<List<?>> expired = cache.query(new SqlFieldsQuery("SELECT * FROM \"notEager\".Integer where _key = 42")).getAll();

            Assert.assertTrue("Expired row should not be returned by sql. Result = " + expired, expired.isEmpty());
        }

        {
            List<List<?>> expired = cache.query(new SqlFieldsQuery("SELECT * FROM \"notEager\".Integer where id >= 42 and id <= 42")).getAll();

            Assert.assertTrue("Expired row should not be returned by sql. Result = " + expired, expired.isEmpty());
        }

        {
            List<List<?>> expired = cache.query(new SqlFieldsQuery("SELECT * FROM \"notEager\".Integer where id >= 5 and id <= 5")).getAll();

            assertEqualsCollections(Collections.singletonList(asList(5, 6)), expired);
        }
    }

    /**
     * Expired row check of the tree index in case {@link H2TreeIndex#find(Session, SearchRow, SearchRow)} doesn't
     * perform one-row optimization and returns {@link H2Cursor}.
     */
    @Test
    public void testTreeIndexManyRows() throws Exception {
        IgniteCache<Integer, Integer> cache = createTestCache();

        cache.put(1, 2);
        cache.put(3, 4);

        putExpireInYear(cache, 5, 6);

        putExpiredSoon(cache, 42, 43);
        putExpiredSoon(cache, 77, 88);

        U.sleep(WAIT_MS_TIL_EXPIRED);

        {
            List<List<?>> mixed = cache.query(new SqlFieldsQuery("SELECT * FROM \"notEager\".Integer WHERE id >= 5")).getAll();

            assertEqualsCollections(Collections.singletonList(asList(5, 6)), mixed);
        }

        {
            List<List<?>> mixed = cache.query(new SqlFieldsQuery("SELECT * FROM \"notEager\".Integer WHERE id >= 3")).getAll();

            assertEqualsCollections(asList(asList(3, 4), asList(5, 6)), mixed);
        }

        {
            List<List<?>> expired = cache.query(new SqlFieldsQuery("SELECT * FROM \"notEager\".Integer WHERE id >= 42")).getAll();

            Assert.assertTrue("Expired row should not be returned by sql. Result = " + expired, expired.isEmpty());
        }
    }

    /**
     * Expired row check if hash index is used.
     */
    @Test
    public void testHashIndex() throws Exception {
        IgniteCache<Integer, Integer> cache = createTestCache();

        cache.put(1, 2);
        cache.put(3, 4);

        putExpireInYear(cache, 5, 6);

        putExpiredSoon(cache, 42, 43);
        putExpiredSoon(cache, 77, 88);

        U.sleep(WAIT_MS_TIL_EXPIRED);

        List<List<?>> mixed = cache.query(new SqlFieldsQuery(
            "SELECT * FROM \"notEager\".Integer USE INDEX (\"_key_PK_hash\")")).getAll();

        List<List<Integer>> exp = asList(
            asList(1, 2),
            asList(3, 4),
            asList(5, 6));

        assertEqualsCollections(exp, mixed);

        List<List<?>> expired = cache.query(new SqlFieldsQuery(
            "SELECT * FROM \"notEager\".Integer USE INDEX (\"_key_PK_hash\") WHERE id >= 42 and id <= 42")).getAll();

        Assert.assertTrue("Expired row should not be returned by sql. Result = " + expired, expired.isEmpty());
    }

    @Test
    public void testDataExpiredAfterInsertFromSelect() throws Exception {
        int duration = 2_000;
        IgniteEx grid = startGrid(1);

        try {

            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();
            ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            ccfg.setCacheMode(CacheMode.REPLICATED);
            ccfg.setName("temp");
            ccfg.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_DAY));
            IgniteCache<Object, Object> cache = grid.getOrCreateCache(ccfg);

            CacheConfiguration<Object, Object> ccfg1 = new CacheConfiguration<>();
            ccfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            ccfg1.setCacheMode(CacheMode.REPLICATED);
            ccfg1.setName("person");
            ccfg1.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, duration)));
            grid.getOrCreateCache(ccfg1);

            FieldsQueryCursor<List<?>> res = cache.query(new SqlFieldsQuery(
                "CREATE TABLE IF NOT EXISTS temp(\n" +
                    "    id VARCHAR,\n" +
                    "    updated TIMESTAMP,\n" +
                    "    PRIMARY KEY (id)\n" +
                    ") WITH \"\n" +
                    "    value_type=person_type,\n" +
                    "    cache_name=temp\n" +
                    "\""));

            assertNotNull(res);

            res.close();

            for (int i = 0; i < 10; ++i) {
                cache.query(new SqlFieldsQuery(
                    String.format("INSERT INTO temp(id, updated) VALUES ('%d', CURRENT_TIMESTAMP())", i)));
            }

            cache.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS person(\n" +
                "    id VARCHAR,\n" +
                "    updated TIMESTAMP,\n" +
                "    PRIMARY KEY (id)\n" +
                ") WITH \"\n" +
                "    value_type=person_type,\n" +
                "    cache_name=person\n" +
                "\""));

            cache.query(new SqlFieldsQuery("INSERT INTO person(id, updated)\n" +
                "SELECT id, updated\n" +
                "FROM temp;"));

            cache.query(new SqlFieldsQuery("INSERT INTO person(id, updated) VALUES ('11', CURRENT_TIMESTAMP());"));

            // wait all data will be expired.
            U.sleep(2 * duration);

            res = cache.query(new SqlFieldsQuery("SELECT * FROM person order by id;"));

            assertTrue("Unexpected resultset.", res.getAll().isEmpty());

            res.close();
        } finally {
            grid.close();
        }
    }
}

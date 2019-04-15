/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.index;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.engine.Session;
import org.h2.result.SearchRow;
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
        startGrids(1);
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

}

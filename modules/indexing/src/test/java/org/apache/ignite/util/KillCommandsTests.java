/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.util;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.event.CacheEntryEvent;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.util.KillCommandsSQLTest.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the cancel command.
 */
class KillCommandsTests {
    /** Service name. */
    public static final String SVC_NAME = "my-svc";

    /** Cache name. */
    public static final String DEFAULT_CACHE_NAME = "default";

    /** Page size. */
    public static final int PAGE_SZ = 5;

    /** Operations timeout. */
    public static final int TIMEOUT = 10_000;

    /**
     * Test cancel of the SQL query.
     *
     * @param cli Client node.
     * @param qryCanceler Query cancel closure.
     */
    public static void doTestCancelSQLQuery(IgniteEx cli, Consumer<String> qryCanceler) {
        String qryStr = "SELECT * FROM \"default\".Integer";

        SqlFieldsQuery qry = new SqlFieldsQuery(qryStr).setPageSize(PAGE_SZ);
        Iterator<List<?>> iter = AbstractSchemaSelfTest.queryProcessor(cli).querySqlFields(qry, true).iterator();

        assertNotNull(iter.next());

        List<List<?>> sqlQries = execute(cli, "SELECT * FROM SYS.SQL_QUERIES ORDER BY START_TIME");

        assertEquals(2, sqlQries.size());

        String qryId = (String)sqlQries.get(0).get(0);

        assertEquals(qryStr, sqlQries.get(0).get(1));

        qryCanceler.accept(qryId);

        for (int i = 0; i < PAGE_SZ - 2; i++)
            assertNotNull(iter.next());

        assertThrowsWithCause(iter::next, CacheException.class);
    }

    /**
     * Test cancel of the continuous query.
     *
     * @param cli Client node.
     * @param srvs Server nodes.
     * @param qryCanceler Query cancel closure.
     */
    public static void doTestCancelContinuousQuery(IgniteEx cli, List<IgniteEx> srvs,
        BiConsumer<UUID, UUID> qryCanceler) throws Exception {
        IgniteCache<Object, Object> cache = cli.cache(DEFAULT_CACHE_NAME);

        ContinuousQuery<Integer, Integer> cq = new ContinuousQuery<>();

        AtomicInteger cntr = new AtomicInteger();

        cq.setInitialQuery(new ScanQuery<>());
        cq.setTimeInterval(1_000L);
        cq.setPageSize(PAGE_SZ);
        cq.setLocalListener(events -> {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> e : events) {
                assertNotNull(e);

                cntr.incrementAndGet();
            }
        });

        cache.query(cq);

        for (int i = 0; i < PAGE_SZ * PAGE_SZ; i++)
            cache.put(i, i);

        boolean res = waitForCondition(() -> cntr.get() == PAGE_SZ * PAGE_SZ, TIMEOUT);
        assertTrue(res);

        List<List<?>> cqQries = execute(cli,
            "SELECT NODE_ID, ROUTINE_ID FROM SYS.CONTINUOUS_QUERIES");
        assertEquals(1, cqQries.size());

        UUID nodeId = (UUID)cqQries.get(0).get(0);
        UUID routineId = (UUID)cqQries.get(0).get(1);

        qryCanceler.accept(nodeId, routineId);

        long cnt = cntr.get();

        for (int i = 0; i < PAGE_SZ * PAGE_SZ; i++)
            cache.put(i, i);

        res = waitForCondition(() -> cntr.get() > cnt, TIMEOUT);

        assertFalse(res);

        for (int i = 0; i < srvs.size(); i++) {
            IgniteEx srv = srvs.get(i);

            res = waitForCondition(() -> execute(srv,
                "SELECT ROUTINE_ID FROM SYS.CONTINUOUS_QUERIES").isEmpty(), TIMEOUT);

            assertTrue(srv.configuration().getIgniteInstanceName(), res);
        }
    }

    /**
     * Test cancel of the scan query.
     *
     * @param cli Client node.
     * @param srvs Server nodes.
     * @param qryCanceler Query cancel closure.
     */
    public static void doTestScanQueryCancel(IgniteEx cli, List<IgniteEx> srvs, Consumer<T3<UUID, String, Long>> qryCanceler) {
        IgniteCache<Object, Object> cache = cli.cache(DEFAULT_CACHE_NAME);

        QueryCursor<Cache.Entry<Object, Object>> qry1 = cache.query(new ScanQuery<>().setPageSize(PAGE_SZ));
        Iterator<Cache.Entry<Object, Object>> iter1 = qry1.iterator();

        // Fetch first entry and therefore caching first page.
        assertNotNull(iter1.next());

        List<List<?>> scanQries0 = execute(srvs.get(0),
            "SELECT ORIGIN_NODE_ID, CACHE_NAME, QUERY_ID FROM SYS.SCAN_QUERIES");

        assertEquals(1, scanQries0.size());

        UUID originNodeId = (UUID)scanQries0.get(0).get(0);
        String cacheName = (String)scanQries0.get(0).get(1);
        long qryId = (Long)scanQries0.get(0).get(2);

        // Opens second query.
        QueryCursor<Cache.Entry<Object, Object>> qry2 = cache.query(new ScanQuery<>().setPageSize(PAGE_SZ));
        Iterator<Cache.Entry<Object, Object>> iter2 = qry2.iterator();

        // Fetch first entry and therefore caching first page.
        assertNotNull(iter2.next());

        // Cancel first query.
        qryCanceler.accept(new T3<>(originNodeId, cacheName, qryId));

        // Fetch all cached entries. It's size equal to the {@code PAGE_SZ * NODES_CNT}.
        for (int i = 0; i < PAGE_SZ * srvs.size() - 1; i++)
            assertNotNull(iter1.next());

        // Fetch of the next page should throw the exception.
        assertThrowsWithCause(iter1::next, IgniteCheckedException.class);

        // Checking that second query works fine after canceling first.
        for (int i = 0; i < PAGE_SZ * PAGE_SZ - 1; i++)
            assertNotNull(iter2.next());

        // Checking all server node objects cleared after cancel.
        for (int i = 0; i < srvs.size(); i++) {
            IgniteEx ignite = srvs.get(i);

            int cacheId = CU.cacheId(DEFAULT_CACHE_NAME);

            GridCacheContext<?, ?> ctx = ignite.context().cache().context().cacheContext(cacheId);

            ConcurrentMap<UUID, ? extends GridCacheQueryManager<?, ?>.RequestFutureMap> qryIters =
                ctx.queries().queryIterators();

            assertTrue(qryIters.size() <= 1);

            if (qryIters.isEmpty())
                return;

            GridCacheQueryManager<?, ?>.RequestFutureMap futs = qryIters.get(cli.localNode().id());

            assertNotNull(futs);
            assertFalse(futs.containsKey(qryId));
        }
    }
}

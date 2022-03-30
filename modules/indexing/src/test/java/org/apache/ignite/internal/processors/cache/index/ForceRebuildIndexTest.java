/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryRetryException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.StopBuildIndexConsumer;
import org.apache.ignite.internal.processors.query.aware.IndexRebuildFutureStorage;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static org.apache.ignite.internal.processors.cache.index.IgniteH2IndexingEx.prepareBeforeNodeStart;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Class for testing forced rebuilding of indexes.
 */
public class ForceRebuildIndexTest extends AbstractRebuildIndexTest {
    /** */
    private static final Semaphore hook = new Semaphore(0);

    /**
     * Checking that a forced rebuild of indexes is possible only after the previous one has finished.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSequentialForceRebuildIndexes() throws Exception {
        prepareBeforeNodeStart();

        IgniteEx n = startGrid(0);

        populate(n.cache(DEFAULT_CACHE_NAME), 100);

        GridCacheContext<?, ?> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        StopBuildIndexConsumer stopRebuildIdxConsumer = addStopRebuildIndexConsumer(n, cacheCtx.name());

        // The forced rebuild has begun - no rejected.
        assertEqualsCollections(emptyList(), forceRebuildIndexes(n, cacheCtx));

        IgniteInternalFuture<?> idxRebFut0 = checkStartRebuildIndexes(n, cacheCtx);

        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());
        assertFalse(idxRebFut0.isDone());

        // There will be no forced rebuilding since the previous one has not ended - they will be rejected.
        assertEqualsCollections(F.asList(cacheCtx), forceRebuildIndexes(n, cacheCtx));
        assertTrue(idxRebFut0 == indexRebuildFuture(n, cacheCtx.cacheId()));

        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();

        idxRebFut0.get(getTestTimeout());

        checkFinishRebuildIndexes(n, cacheCtx, 100);
        assertEquals(100, stopRebuildIdxConsumer.visitCnt.get());

        stopRebuildIdxConsumer.resetFutures();

        // Forced rebuilding is possible again as the past is over - no rejected.
        assertEqualsCollections(emptyList(), forceRebuildIndexes(n, cacheCtx));

        IgniteInternalFuture<?> idxRebFut1 = checkStartRebuildIndexes(n, cacheCtx);

        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());
        assertFalse(idxRebFut1.isDone());

        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();
        idxRebFut1.get(getTestTimeout());

        checkFinishRebuildIndexes(n, cacheCtx, 100);
        assertEquals(200, stopRebuildIdxConsumer.visitCnt.get());
    }

    /**
     * Test verify that after rebuild all corrupted entries will be removed from the index.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorruptedIndexRebuild() throws Exception {
        IgniteEx ignite = startGrid(0);

        sql("CREATE TABLE test_tbl (id1 INTEGER, id2 INTEGER, val VARCHAR, CONSTRAINT pk PRIMARY KEY (id1, id2))"
                + " WITH \"cache_name=TEST_CACHE, key_type=KEY, value_type=VAL\"");

        sql("CREATE INDEX test_tbl_val_idx ON test_tbl(val)");

        // the keys below are logically equal, but physically (i.e. as byte arrays) are not
        BinaryObject key1 = ignite.binary().builder("KEY").setField("id1", 1).setField("id2", 2).build();
        BinaryObject key2 = ignite.binary().builder("KEY").setField("id2", 2).setField("id1", 1).build();
        BinaryObject val = ignite.binary().builder("VAL").setField("val", "val").build();

        IgniteCache<BinaryObject, BinaryObject> cache = ignite.cache("TEST_CACHE").withKeepBinary();
        cache.put(key1, val);
        cache.put(key2, val);

        assertEquals(1L, sql("SELECT count(id1) FROM test_tbl USE INDEX (\"_key_PK\")").get(0).get(0));
        assertEquals(2L, sql("SELECT count(val) FROM test_tbl USE INDEX (test_tbl_val_idx)").get(0).get(0));
        assertEquals(2, cache.size());

        cache.remove(key1);
        cache.remove(key2);

        assertEquals(0L, sql("SELECT count(id1) FROM test_tbl USE INDEX (\"_key_PK\")").get(0).get(0));
        assertEquals(0, cache.size());
        assertThrowsWithCause(
                () -> sql("SELECT count(val) FROM test_tbl USE INDEX (test_tbl_val_idx)").get(0).get(0),
                Exception.class
        );

        for (int i = 0; i < 10; i++) {
            BinaryObject key = ignite.binary().builder("KEY")
                    .setField("id1", i * 10)
                    .setField("id2", i * 10)
                    .build();
            BinaryObject val0 = ignite.binary().builder("VAL")
                    .setField("val", "val")
                    .build();

            cache.put(key, val0);
        }

        GridCacheContext<?, ?> cacheCtx = ignite.cachex("TEST_CACHE").context();

        forceRebuildIndexes(ignite, cacheCtx);

        IgniteInternalFuture<?> fut = indexRebuildFuture(ignite, cacheCtx.cacheId());

        if (fut != null)
            fut.get(getTestTimeout());

        assertEquals(10L, sql("SELECT count(val) FROM test_tbl USE INDEX (test_tbl_val_idx)").get(0).get(0));
    }

    /**
     * In case of lazy queries, if rebuild happened in the middle, such query should be failed with {@link QueryRetryException}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLazyQueryShouldBeProperlyCancelled() throws Exception {
        IgniteEx ignite = startGrid(0);

        sql("CREATE TABLE test_tbl (id INTEGER, aff_key INTEGER, val VARCHAR, CONSTRAINT pk PRIMARY KEY (id, aff_key))"
                + " WITH \"cache_name=TEST_CACHE, affinity_key=aff_key\"");
        sql("CREATE INDEX test_tbl_val_idx ON test_tbl(val)");

        for (int i = 0; i < 100_000; i++)
            sql("INSERT INTO test_tbl VALUES (?, ?, ?)", i, i, "val_" + i);

        Iterator<?> curByPk = openCursor("SELECT id FROM test_tbl USE INDEX (\"_key_PK\") WHERE id > -1").iterator();
        Iterator<?> curByAff = openCursor("SELECT aff_key FROM test_tbl USE INDEX (AFFINITY_KEY) WHERE aff_key > -1").iterator();
        Iterator<?> curBySecIdx = openCursor("SELECT val FROM test_tbl USE INDEX (test_tbl_val_idx) WHERE VAL > 'val'").iterator();

        curByPk.next();
        curByAff.next();
        curBySecIdx.next();

        GridCacheContext<?, ?> cacheCtx = ignite.cachex("TEST_CACHE").context();

        forceRebuildIndexes(ignite, cacheCtx);

        IgniteInternalFuture<?> fut = indexRebuildFuture(ignite, cacheCtx.cacheId());

        if (fut != null)
            fut.get(getTestTimeout());

        assertThrowsWithCause(() -> drainIterator(curByPk), QueryRetryException.class);
        assertThrowsWithCause(() -> drainIterator(curByAff), QueryRetryException.class);
        assertThrowsWithCause(() -> drainIterator(curBySecIdx), QueryRetryException.class);
    }

    /**
     * In case of non-lazy queries, if rebuild should wait till the query completes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNonLazyQueryShouldCompleteNormally() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.getOrCreateCache(new CacheConfiguration<>("DUMMY")
                .setSqlSchema("PUBLIC")
                .setSqlFunctionClasses(this.getClass())
        );

        sql("CREATE TABLE test_tbl (id INTEGER PRIMARY KEY, val VARCHAR) WITH \"cache_name=TEST_CACHE\"");

        for (int i = 0; i < 10_000; i++)
            sql("INSERT INTO test_tbl VALUES (?, ?)", i, "val_" + i);

        hook.drainPermits();

        IgniteInternalFuture<?> qryFut = runAsync(() -> sql("SELECT id FROM test_tbl WHERE hook() ORDER BY val"));

        assertTrue(waitForCondition(hook::hasQueuedThreads, 2_000));

        GridCacheContext<?, ?> cacheCtx = ignite.cachex("TEST_CACHE").context();

        IgniteInternalFuture<?> startRebuildFut = runAsync(() -> forceRebuildIndexes(ignite, cacheCtx));

        assertFalse(startRebuildFut.isDone());

        hook.release(Integer.MAX_VALUE);

        startRebuildFut.get(getTestTimeout());

        IgniteInternalFuture<?> fut = indexRebuildFuture(ignite, cacheCtx.cacheId());

        assertNotNull(qryFut);
        qryFut.get(getTestTimeout());

        fut.get(getTestTimeout());
    }

    /**
     * Checking that a forced index rebuild can only be performed after an index rebuild after an exchange.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testForceRebuildIndexesAfterExchange() throws Exception {
        IgniteEx n = startGrid(0);

        populate(n.cache(DEFAULT_CACHE_NAME), 100);

        stopAllGridsWithDeleteIndexBin();

        prepareBeforeNodeStart();

        StopBuildIndexConsumer stopRebuildIdxConsumer = addStopRebuildIndexConsumer(n, DEFAULT_CACHE_NAME);

        n = startGrid(0);

        GridCacheContext<?, ?> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());

        IgniteInternalFuture<?> idxRebFut0 = checkStartRebuildIndexes(n, cacheCtx);
        checkRebuildAfterExchange(n, cacheCtx.cacheId(), true);

        // There will be no forced rebuilding of indexes since it has not ended after the exchange - they will be rejected.
        assertEqualsCollections(F.asList(cacheCtx), forceRebuildIndexes(n, cacheCtx));
        assertTrue(idxRebFut0 == indexRebuildFuture(n, cacheCtx.cacheId()));
        checkRebuildAfterExchange(n, cacheCtx.cacheId(), true);

        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();

        idxRebFut0.get(getTestTimeout());

        checkFinishRebuildIndexes(n, cacheCtx, 100);
        assertEquals(100, stopRebuildIdxConsumer.visitCnt.get());
        checkRebuildAfterExchange(n, cacheCtx.cacheId(), false);

        stopRebuildIdxConsumer.resetFutures();

        // A forced index rebuild will be triggered because it has ended after the exchange - no rejected.
        assertEqualsCollections(emptyList(), forceRebuildIndexes(n, cacheCtx));

        IgniteInternalFuture<?> idxRebFut1 = checkStartRebuildIndexes(n, cacheCtx);
        checkRebuildAfterExchange(n, cacheCtx.cacheId(), false);

        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());
        assertFalse(idxRebFut1.isDone());

        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();
        idxRebFut1.get(getTestTimeout());

        checkFinishRebuildIndexes(n, cacheCtx, 100);
        checkRebuildAfterExchange(n, cacheCtx.cacheId(), false);
        assertEquals(200, stopRebuildIdxConsumer.visitCnt.get());
    }

    /**
     * Checking that sequential index rebuilds on exchanges will not intersection.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSequentialRebuildIndexesOnExchange() throws Exception {
        IgniteEx n = startGrid(0);

        populate(n.cache(DEFAULT_CACHE_NAME), 100);

        stopAllGridsWithDeleteIndexBin();

        prepareBeforeNodeStart();

        StopBuildIndexConsumer stopRebuildIdxConsumer = addStopRebuildIndexConsumer(n, DEFAULT_CACHE_NAME);

        n = startGrid(0);

        GridCacheContext<?, ?> cacheCtx = n.cachex(DEFAULT_CACHE_NAME).context();

        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());

        IgniteInternalFuture<?> idxRebFut = checkStartRebuildIndexes(n, cacheCtx);

        // To initiate an exchange.
        n.getOrCreateCache(DEFAULT_CACHE_NAME + "_1");

        assertTrue(idxRebFut == indexRebuildFuture(n, cacheCtx.cacheId()));

        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();

        idxRebFut.get(getTestTimeout());

        checkFinishRebuildIndexes(n, cacheCtx, 100);
        assertEquals(100, stopRebuildIdxConsumer.visitCnt.get());
    }

    /**
     * Checking the contents of the cache in {@code GridQueryProcessor#idxRebuildOnExchange}.
     * Allows to check if the cache will be marked, that the rebuild for it should be after the exchange.
     *
     * @param n Node.
     * @param cacheId Cache id.
     * @param expContains Whether a cache is expected.
     */
    private void checkRebuildAfterExchange(IgniteEx n, int cacheId, boolean expContains) {
        IndexRebuildFutureStorage idxRebuildAware = getFieldValue(n.context().query(), "idxRebuildFutStorage");

        GridDhtPartitionsExchangeFuture exhFut = n.context().cache().context().exchange().lastTopologyFuture();

        assertEquals(expContains, idxRebuildAware.rebuildIndexesOnExchange(cacheId, exhFut.initialVersion()));
    }

    /**
     * Executes the given query and returns the result.
     *
     * @param qry Query string to execute.
     * @param args Arguments for the query
     * @return The result.
     */
    private List<List<?>> sql(String qry, Object... args) {
        return openCursor(qry, args).getAll();
    }

    /**
     * Opens cursor for the given query.
     *
     * @param qry Query to open cursor for.
     * @param args Arguments for the query
     * @return Opened cursor.
     */
    private FieldsQueryCursor<List<?>> openCursor(String qry, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(qry)
                .setArgs(args).setLazy(true), true);
    }

    /**
     * Fetches all remaining items from the given cursor.
     *
     * @param it Iterator to drain.
     */
    private static void drainIterator(Iterator<?> it) {
        while (it.hasNext())
            it.next();
    }

    @QuerySqlFunction
    public static boolean hook() {
        try {
            hook.acquire();
        }
        catch (InterruptedException e) {
            // NO-OP
        }
        finally {
            hook.release();
        }

        return true;
    }
}

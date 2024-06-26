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

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.BreakBuildIndexConsumer;
import org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.SlowdownBuildIndexConsumer;
import org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.StopBuildIndexConsumer;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryIndexKey;
import org.apache.ignite.internal.processors.query.aware.IndexBuildStatusHolder;
import org.apache.ignite.internal.processors.query.aware.IndexBuildStatusHolder.Status;
import org.apache.ignite.internal.processors.query.h2.DurableBackgroundCleanupIndexTreeTaskV2;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_INDEX_REBUILD_BATCH_SIZE;
import static org.apache.ignite.internal.processors.cache.index.IgniteH2IndexingEx.addIdxCreateCacheRowConsumer;
import static org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.nodeName;
import static org.apache.ignite.internal.processors.query.aware.IndexBuildStatusHolder.Status.COMPLETE;
import static org.apache.ignite.internal.processors.query.aware.IndexBuildStatusHolder.Status.INIT;
import static org.apache.ignite.internal.processors.query.aware.IndexBuildStatusStorage.KEY_PREFIX;
import static org.apache.ignite.internal.processors.query.h2.DurableBackgroundCleanupIndexTreeTaskV2.idxTreeFactory;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Test to check consistency when adding a new index.
 */
@WithSystemProperty(key = IGNITE_INDEX_REBUILD_BATCH_SIZE, value = "1")
public class ResumeCreateIndexTest extends AbstractRebuildIndexTest {
    /** Original {@link DurableBackgroundCleanupIndexTreeTaskV2#idxTreeFactory}. */
    private DurableBackgroundCleanupIndexTreeTaskV2.H2TreeFactory originalTaskIdxTreeFactory;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        originalTaskIdxTreeFactory = idxTreeFactory;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        idxTreeFactory = originalTaskIdxTreeFactory;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                cacheConfig(DEFAULT_CACHE_NAME),
                // Add one more cache to keep CacheGroup non-empty when the first cache will be destroyed during test.
                cacheConfig(DEFAULT_CACHE_NAME + 2)
            );
    }

    /** */
    private CacheConfiguration<Object, Object> cacheConfig(String cacheName) {
        return cacheCfg(cacheName, "GRP").setAffinity(new RendezvousAffinityFunction(false, 1));
    }

    /**
     * Checking the general flow of building a new index.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGeneralFlow() throws Exception {
        String cacheName = DEFAULT_CACHE_NAME;

        IgniteEx n = prepareNodeToCreateNewIndex(cacheName, 10, true);

        String idxName = "IDX0";
        SlowdownBuildIndexConsumer slowdownIdxCreateConsumer = addSlowdownIdxCreateConsumer(n, idxName, 0);

        IgniteInternalFuture<List<List<?>>> createIdxFut = createIdxAsync(n.cache(cacheName), idxName);

        slowdownIdxCreateConsumer.startBuildIdxFut.get(getTestTimeout());

        checkInitStatus(n, cacheName, false, 1);

        slowdownIdxCreateConsumer.finishBuildIdxFut.onDone();
        createIdxFut.get(getTestTimeout());

        checkCompletedStatus(n, cacheName);

        enableCheckpointsAsync(n, getTestIgniteInstanceName(), true).get(getTestTimeout());

        checkNoStatus(n, cacheName);
    }

    /**
     * Checks that if there is no checkpoint after the index is created and the
     * node is restarted, the indexes will be rebuilt.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoCheckpointAfterIndexCreation() throws Exception {
        String cacheName = DEFAULT_CACHE_NAME;

        IgniteEx n = prepareNodeToCreateNewIndex(cacheName, 10, true);

        String idxName = "IDX0";
        SlowdownBuildIndexConsumer slowdownIdxCreateConsumer = addSlowdownIdxCreateConsumer(n, idxName, 0);

        IgniteInternalFuture<List<List<?>>> createIdxFut = createIdxAsync(n.cache(cacheName), idxName);

        slowdownIdxCreateConsumer.startBuildIdxFut.get(getTestTimeout());

        checkInitStatus(n, cacheName, false, 1);

        slowdownIdxCreateConsumer.finishBuildIdxFut.onDone();
        createIdxFut.get(getTestTimeout());

        checkCompletedStatus(n, cacheName);

        stopGrid(0);

        IgniteH2IndexingEx.prepareBeforeNodeStart();
        StopBuildIndexConsumer stopRebuildIdxConsumer = addStopRebuildIndexConsumer(n, cacheName);

        n = startGrid(0);
        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());

        IgniteInternalFuture<?> idxRebFut = indexRebuildFuture(n, CU.cacheId(cacheName));
        assertNotNull(idxRebFut);

        checkInitStatus(n, cacheName, true, 0);
        assertTrue(allIndexes(n).containsKey(new QueryIndexKey(cacheName, idxName)));

        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();
        idxRebFut.get(getTestTimeout());

        forceCheckpoint();

        checkNoStatus(n, cacheName);
        assertEquals(10, selectPersonByName(n.cache(cacheName)).size());
    }

    /**
     * Checks that if errors occur while building a new index, then there will be no rebuilding of the indexes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testErrorFlow() throws Exception {
        String cacheName = DEFAULT_CACHE_NAME;

        IgniteEx n = prepareNodeToCreateNewIndex(cacheName, 10, true);

        String idxName = "IDX0";
        BreakBuildIndexConsumer breakBuildIdxConsumer = addBreakIdxCreateConsumer(n, idxName, 1);

        IgniteInternalFuture<List<List<?>>> createIdxFut = createIdxAsync(n.cache(cacheName), idxName);

        breakBuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());

        checkInitStatus(n, cacheName, false, 1);

        breakBuildIdxConsumer.finishBuildIdxFut.onDone();
        assertThrows(log, () -> createIdxFut.get(getTestTimeout()), IgniteCheckedException.class, null);

        checkCompletedStatus(n, cacheName);

        enableCheckpointsAsync(n, getTestIgniteInstanceName(), true).get(getTestTimeout());

        checkNoStatus(n, cacheName);
    }

    /**
     * Checks that building a new index and rebuilding indexes at the same time
     * does not break the {@link IndexBuildStatusHolder}.
     * In this case, building a new index is completed earlier.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentBuildNewIndexAndRebuildIndexes0() throws Exception {
        String cacheName = DEFAULT_CACHE_NAME;

        IgniteEx n = prepareNodeToCreateNewIndex(cacheName, 100_000, true);

        String idxName = "IDX0";
        SlowdownBuildIndexConsumer slowdownIdxCreateConsumer = addSlowdownIdxCreateConsumer(n, idxName, 0);

        IgniteInternalFuture<List<List<?>>> createIdxFut = createIdxAsync(n.cache(cacheName), idxName);

        slowdownIdxCreateConsumer.startBuildIdxFut.get(getTestTimeout());

        checkInitStatus(n, cacheName, false, 1);

        SlowdownBuildIndexConsumer slowdownRebuildIdxConsumer = addSlowdownRebuildIndexConsumer(n, cacheName, 100);
        assertTrue(forceRebuildIndexes(n, n.cachex(cacheName).context()).isEmpty());

        checkInitStatus(n, cacheName, true, 1);

        IgniteInternalFuture<?> idxRebFut = indexRebuildFuture(n, CU.cacheId(cacheName));
        assertNotNull(idxRebFut);

        slowdownIdxCreateConsumer.finishBuildIdxFut.onDone();

        slowdownRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());
        slowdownRebuildIdxConsumer.finishBuildIdxFut.onDone();

        createIdxFut.get(getTestTimeout());

        assertFalse(idxRebFut.isDone());
        checkInitStatus(n, cacheName, true, 0);

        slowdownRebuildIdxConsumer.sleepTime.set(0);
        idxRebFut.get(getTestTimeout());

        checkCompletedStatus(n, cacheName);

        enableCheckpointsAsync(n, getTestIgniteInstanceName(), true).get(getTestTimeout());

        checkNoStatus(n, cacheName);
    }

    /**
     * Checks that building a new index and rebuilding indexes at the same time
     * does not break the {@link IndexBuildStatusHolder}.
     * In this case, rebuilding indexes is completed earlier.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentBuildNewIndexAndRebuildIndexes1() throws Exception {
        String cacheName = DEFAULT_CACHE_NAME;

        IgniteEx n = prepareNodeToCreateNewIndex(cacheName, 100_000, true);

        SlowdownBuildIndexConsumer slowdownRebuildIdxConsumer = addSlowdownRebuildIndexConsumer(n, cacheName, 10);
        assertTrue(forceRebuildIndexes(n, n.cachex(cacheName).context()).isEmpty());

        checkInitStatus(n, cacheName, true, 0);

        slowdownRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());

        IgniteInternalFuture<?> idxRebFut = indexRebuildFuture(n, CU.cacheId(cacheName));
        assertNotNull(idxRebFut);

        String idxName = "IDX0";
        SlowdownBuildIndexConsumer slowdownIdxCreateConsumer = addSlowdownIdxCreateConsumer(n, idxName, 100);

        IgniteInternalFuture<List<List<?>>> createIdxFut = createIdxAsync(n.cache(cacheName), idxName);

        slowdownRebuildIdxConsumer.finishBuildIdxFut.onDone();
        slowdownIdxCreateConsumer.startBuildIdxFut.get(getTestTimeout());

        checkInitStatus(n, cacheName, true, 1);

        slowdownIdxCreateConsumer.finishBuildIdxFut.onDone();
        slowdownRebuildIdxConsumer.sleepTime.set(0);
        idxRebFut.get(getTestTimeout());

        checkInitStatus(n, cacheName, false, 1);

        slowdownIdxCreateConsumer.sleepTime.set(0);
        createIdxFut.get(getTestTimeout());

        checkCompletedStatus(n, cacheName);

        enableCheckpointsAsync(n, getTestIgniteInstanceName(), true).get(getTestTimeout());

        checkNoStatus(n, cacheName);
    }

    /**
     * Checks that if a checkpoint fails after building a new index and the
     * node restarts, then the indexes will be rebuilt.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartialCheckpointNewIndexRows() throws Exception {
        String cacheName = DEFAULT_CACHE_NAME;

        IgniteEx n = prepareNodeToCreateNewIndex(cacheName, 100_000, false);

        String idxName = "IDX0";
        SlowdownBuildIndexConsumer slowdownIdxCreateConsumer = addSlowdownIdxCreateConsumer(n, idxName, 10);

        IgniteInternalFuture<List<List<?>>> createIdxFut = createIdxAsync(n.cache(cacheName), idxName);

        slowdownIdxCreateConsumer.startBuildIdxFut.get(getTestTimeout());

        checkInitStatus(n, cacheName, false, 1);

        String reason = getTestIgniteInstanceName();
        IgniteInternalFuture<Void> awaitBeforeCpBeginFut = awaitBeforeCheckpointBeginAsync(n, reason);
        IgniteInternalFuture<Void> disableCpFut = enableCheckpointsAsync(n, reason, false);

        awaitBeforeCpBeginFut.get(getTestTimeout());
        slowdownIdxCreateConsumer.finishBuildIdxFut.onDone();

        disableCpFut.get(getTestTimeout());
        slowdownIdxCreateConsumer.sleepTime.set(0);

        createIdxFut.get(getTestTimeout());

        checkCompletedStatus(n, cacheName);

        stopGrid(0);

        IgniteH2IndexingEx.prepareBeforeNodeStart();
        StopBuildIndexConsumer stopRebuildIdxConsumer = addStopRebuildIndexConsumer(n, cacheName);

        n = startGrid(0);
        stopRebuildIdxConsumer.startBuildIdxFut.get(getTestTimeout());

        IgniteInternalFuture<?> rebIdxFut = indexRebuildFuture(n, CU.cacheId(cacheName));
        assertNotNull(rebIdxFut);

        checkInitStatus(n, cacheName, true, 0);
        assertTrue(allIndexes(n).containsKey(new QueryIndexKey(cacheName, idxName)));

        stopRebuildIdxConsumer.finishBuildIdxFut.onDone();
        rebIdxFut.get(getTestTimeout());

        forceCheckpoint();

        checkNoStatus(n, cacheName);
        assertEquals(100_000, selectPersonByName(n.cache(cacheName)).size());
    }

    /**
     * Checks that incomplete index is destroyed.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
    @Test
    public void testIncompleteIndexDroppedOnCacheDestroy() throws Exception {
        final String cacheName = DEFAULT_CACHE_NAME;
        final int cacheSize = 10_000;

        IgniteEx n = prepareNodeToCreateNewIndex(cacheName, cacheSize, false);
        populate(n.cache(DEFAULT_CACHE_NAME + 2), 1);

        IgniteEx cli = startClientGrid(1);

        String idxName = "IDX0";
        StopBuildIndexConsumer failBuildIndexConsumer = new FailBuildIndexConsumer(getTestTimeout(), 1000);
        addIdxCreateCacheRowConsumer(nodeName(n), idxName, failBuildIndexConsumer);

        IgniteInternalFuture<List<List<?>>> createIdxFut = createIdxAsync(cli.cache(cacheName), idxName);

        GridFutureAdapter<Object> startCleanupFut = new GridFutureAdapter<>();
        DurableBackgroundCleanupIndexTreeTaskV2.idxTreeFactory = treeFactory(idxName, startCleanupFut);

        failBuildIndexConsumer.startBuildIdxFut.get(getTestTimeout());
        checkInitStatus(n, cacheName, false, 1);
        failBuildIndexConsumer.finishBuildIdxFut.onDone();

        cli.cache(DEFAULT_CACHE_NAME).destroy();
        assertTrue(createIdxFut.isDone());
        startCleanupFut.get(getTestTimeout());

        cli.createCache(cacheConfig(DEFAULT_CACHE_NAME));
        populate(n.cache(cacheName), cacheSize);

        checkCompletedStatus(n, cacheName);

        StopBuildIndexConsumer slowdownBuildIndexConsumer = addSlowdownIdxCreateConsumer(n, idxName, 0);
        createIdxFut = createIdxAsync(cli.cache(cacheName), idxName);

        slowdownBuildIndexConsumer.startBuildIdxFut.get(getTestTimeout());
        checkInitStatus(n, cacheName, false, 1);
        slowdownBuildIndexConsumer.finishBuildIdxFut.onDone();

        createIdxFut.get(getTestTimeout());

        checkCompletedStatus(n, cacheName);
        assertTrue(allIndexes(n).containsKey(new QueryIndexKey(cacheName, idxName)));

        assertEquals(cacheSize, selectPersonByName(n.cache(cacheName)).size());
    }

    /** */
    private DurableBackgroundCleanupIndexTreeTaskV2.H2TreeFactory treeFactory(
        String indexName,
        GridFutureAdapter<Object> startFut
    ) {
        return new DurableBackgroundCleanupIndexTreeTaskV2.H2TreeFactory() {
            /** {@inheritDoc} */
            @Override protected H2Tree create(
                CacheGroupContext grpCtx,
                RootPage rootPage,
                String treeName,
                String idxName,
                String cacheName
            ) throws IgniteCheckedException {
                if (indexName.equals(idxName))
                    startFut.onDone();

                return super.create(grpCtx, rootPage, treeName, idxName, cacheName);
            }
        };
    }

    /**
     * Consumer that fails building indexes of cache.
     */
    static class FailBuildIndexConsumer extends StopBuildIndexConsumer {
        /** Number of rows to add before slowdown. */
        private final int cnt;

        /**
         * Constructor.
         *
         * @param timeout The maximum time to wait finish future in milliseconds.
         * @param cnt Amount of rows to be added before failure.
         */
        FailBuildIndexConsumer(long timeout, int cnt) {
            super(timeout);

            this.cnt = cnt;
        }

        /** {@inheritDoc} */
        @Override public void accept(CacheDataRow row) throws IgniteCheckedException {
            if (visitCnt.incrementAndGet() < cnt)
                return;

            startBuildIdxFut.onDone();

            finishBuildIdxFut.get(timeout);

            throw new IgniteCheckedException("test");
        }
    }

    /**
     * Asynchronous creation of a new index for the cache of {@link Person}.
     * SQL: CREATE INDEX " + idxName + " ON Person(name)
     *
     * @param cache Cache.
     * @param idxName Index name.
     * @return Index creation future.
     */
    private IgniteInternalFuture<List<List<?>>> createIdxAsync(IgniteCache<Integer, Person> cache, String idxName) {
        return runAsync(() -> createIdx(cache, idxName));
    }

    /**
     * Enable checkpoints asynchronously.
     *
     * @param n Node.
     * @param reason Reason for checkpoint wakeup if it would be required.
     * @param enable Enable/disable.
     * @return Disable checkpoints future.
     */
    private IgniteInternalFuture<Void> enableCheckpointsAsync(IgniteEx n, String reason, boolean enable) {
        return runAsync(() -> enableCheckpoints(n, reason, enable));
    }

    /**
     * Waiting for a {@link CheckpointListener#beforeCheckpointBegin} asynchronously
     * for a checkpoint for a specific reason.
     *
     * @param n Node.
     * @param reason Checkpoint reason.
     * @return Future for waiting for the {@link CheckpointListener#beforeCheckpointBegin}.
     */
    private IgniteInternalFuture<Void> awaitBeforeCheckpointBeginAsync(IgniteEx n, String reason) {
        GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

        dbMgr(n).addCheckpointListener(new CheckpointListener() {
            /** {@inheritDoc} */
            @Override public void onMarkCheckpointBegin(Context ctx) {
                // No-op.
            }

            /** {@inheritDoc} */
            @Override public void onCheckpointBegin(Context ctx) {
                // No-op.
            }

            /** {@inheritDoc} */
            @Override public void beforeCheckpointBegin(Context ctx) {
                if (reason.equals(ctx.progress().reason()))
                    fut.onDone();
            }
        });

        return fut;
    }

    /**
     * Getting {@code GridQueryProcessor#idxs}.
     *
     * @param n Node.
     * @return All indexes.
     */
    private Map<QueryIndexKey, QueryIndexDescriptorImpl> allIndexes(IgniteEx n) {
        return getFieldValue(n.context().query(), "idxs");
    }

    /**
     * Selection of all {@link Person} by name.
     * SQL: SELECT * FROM Person where name LIKE 'name_%';
     *
     * @param cache Cache.
     * @return List containing all query results.
     */
    private List<List<?>> selectPersonByName(IgniteCache<Integer, Person> cache) {
        return cache.query(new SqlFieldsQuery("SELECT * FROM Person where name LIKE 'name_%';")).getAll();
    }

    /**
     * Checking status.
     *
     * @param status Cache index build status.
     * @param expStatus Expected status.
     * @param expPersistent Expected persistence flag.
     * @param expRebuild Expected rebuild flag.
     * @param expNewIdx Expected count of new indexes being built.
     */
    private void checkStatus(
        IndexBuildStatusHolder status,
        Status expStatus,
        boolean expPersistent,
        boolean expRebuild,
        int expNewIdx
    ) {
        assertEquals(expStatus, status.status());
        assertEquals(expPersistent, status.persistent());
        assertEquals(expRebuild, status.rebuild());
        assertEquals(expNewIdx, status.buildNewIndexes());
    }

    /**
     * Creating a node and filling the cache.
     *
     * @param cacheName Cache name.
     * @param cnt Entry count.
     * @param disableCp Disable checkpoint.
     * @return New node.
     * @throws Exception If failed.
     */
    private IgniteEx prepareNodeToCreateNewIndex(String cacheName, int cnt, boolean disableCp) throws Exception {
        IgniteH2IndexingEx.prepareBeforeNodeStart();

        IgniteEx n = startGrid(0);

        populate(n.cache(cacheName), cnt);

        if (disableCp)
            enableCheckpointsAsync(n, getTestIgniteInstanceName(), false).get(getTestTimeout());

        return n;
    }

    /**
     * Checking {@link Status#INIT} status.
     *
     * @param n Node.
     * @param cacheName Cache name.
     * @param expRebuild Expected rebuild flag.
     * @param expNewIdx Expected count of new indexes being built.
     * @throws Exception If failed.
     */
    private void checkInitStatus(IgniteEx n, String cacheName, boolean expRebuild, int expNewIdx) throws Exception {
        checkStatus(statuses(n).get(cacheName), INIT, true, expRebuild, expNewIdx);
        assertNotNull(metaStorageOperation(n, metaStorage -> metaStorage.read(KEY_PREFIX + cacheName)));
        assertEquals(!expRebuild, indexBuildStatusStorage(n).rebuildCompleted(cacheName));
    }

    /**
     * Checking {@link Status#COMPLETE} status.
     *
     * @param n Node.
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void checkCompletedStatus(IgniteEx n, String cacheName) throws Exception {
        checkStatus(statuses(n).get(cacheName), COMPLETE, true, false, 0);
        assertNotNull(metaStorageOperation(n, metaStorage -> metaStorage.read(KEY_PREFIX + cacheName)));
        assertTrue(indexBuildStatusStorage(n).rebuildCompleted(cacheName));
    }

    /**
     * Checking for no status.
     *
     * @param n Node.
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void checkNoStatus(IgniteEx n, String cacheName) throws Exception {
        assertNull(statuses(n).get(cacheName));
        assertNull(metaStorageOperation(n, metaStorage -> metaStorage.read(KEY_PREFIX + cacheName)));
        assertTrue(indexBuildStatusStorage(n).rebuildCompleted(cacheName));
    }
}

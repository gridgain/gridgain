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
package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MessageOrderLogListener;
import org.apache.ignite.testframework.junits.SystemPropertiesList;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.lang.Thread.sleep;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT;

/**
 * Tests case when long index deletion operation happens.
 */
@SystemPropertiesList(
    @WithSystemProperty(key = IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT, value = "5000")
)
public class LongDestroyOperationCheckpointTest extends GridCommonAbstractTest {
    /** */
    private final LogListener blockedSystemCriticalThreadLsnr =
        LogListener.matches("Blocked system-critical thread has been detected").build();

    /** */
    private final LogListener indexDropProcessListener =
        new MessageOrderLogListener(
            ".*?Starting index drop",
            ".*?Checkpoint started.*",
            ".*?Checkpoint finished.*",
            ".*?Index drop completed"
        );

    /** */
    private static final long TIME_FOR_EACH_INDEX_PAGE_TO_DESTROY = 40;

    /** */
    private final AtomicBoolean blockDestroy = new AtomicBoolean(false);

    /** */
    private final ListeningTestLogger testLog =
        new ListeningTestLogger(false, log(), blockedSystemCriticalThreadLsnr, indexDropProcessListener);

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(10 * 1024L * 1024L)
                        .setMaxSize(50 * 1024L * 1024L)
                )
                    .setCheckpointFrequency(Long.MAX_VALUE / 2)
            )
            .setCacheConfiguration(
                new CacheConfiguration(DEFAULT_CACHE_NAME)
                    .setSqlSchema("PUBLIC")
            )
            .setGridLogger(testLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        BPlusTree.destroyClosure = () -> {
            doSleep(TIME_FOR_EACH_INDEX_PAGE_TO_DESTROY);

            if (blockDestroy.compareAndSet(true, false))
                throw new RuntimeException("Aborting destroy.");
        };
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * Tests case when long index deletion operation happens. Checkpoint should run in the middle of index deletion
     * operation. Node can restart without fully deleted index tree.
     *
     * @param restart Whether do restart.
     *
     * @throws Exception If failed.
     */
    private void testLongIndexDeletion(
        boolean restart,
        boolean rebalance,
        boolean multipleNodes,
        boolean multicolumn
    ) throws Exception {
        int nodeCnt = 1;

        IgniteEx ignite = startGrids(1);

        if (multipleNodes) {
            startGrid(1);

            nodeCnt++;
        }

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        query(cache, "create table t (id integer primary key, p integer, f integer, p integer)");

        createIndex(cache, multicolumn);

        for (int i = 0; i < 20_000; i++)
            query(cache, "insert into t (id, p, f) values (?, ?, ?)", i, i, i);

        forceCheckpoint();

        checkSelectAndPlan(cache, true);

        final IgniteCache<Integer, Integer> finalCache = cache;

        Thread dropIdx = new Thread(() -> {
            testLog.info("Starting index drop");

            finalCache.query(new SqlFieldsQuery("drop index t_idx")).getAll();

            testLog.info("Index drop completed");
        });

        indexDropProcessListener.reset();

        dropIdx.start();

        // Waiting for some modified pages
        doSleep(500);

        // Now checkpoint will happen during index deletion before it completes.
        forceCheckpoint();

        if (rebalance) {
            startGrid(nodeCnt);

            nodeCnt++;

            Collection<ClusterNode> blt = IntStream.range(0, nodeCnt)
                .mapToObj(i -> grid(i).localNode())
                .collect(toList());

            ignite.cluster().setBaselineTopology(blt);
        }

        if (restart) {
            blockDestroy.set(true);

            stopGrid(0, true);

            blockDestroy.set(false);

            ignite = startGrid(0);

            if (!multipleNodes)
                ignite.cluster().active(true);

            cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

            try {
                for (int i = 0; i < 20_000; i += 10)
                    cache.query(new SqlFieldsQuery("select id, p from t where p = " + i)).getAll();
            }
            catch (Exception ignored) {
                // No-op: tree is concurrently destroyed.
            }

            sleep(10000);
        }
        else
            dropIdx.join();

        assertFalse(blockedSystemCriticalThreadLsnr.check());

        assertTrue(indexDropProcessListener.check());

        checkSelectAndPlan(cache, false);

        //Trying to recreate index.
        createIndex(cache, multicolumn);

        checkSelectAndPlan(cache, true);
    }

    /** */
    private void checkSelectAndPlan(IgniteCache<Integer, Integer> cache, boolean idxShouldExist) {
        // Ensure that index is not used after it was dropped.
        String plan = query(cache, "explain select id, p from t where p = 0")
            .get(0).get(0).toString();

        assertEquals(plan, idxShouldExist, plan.toUpperCase().contains("T_IDX"));

        // Trying to do a select.
        String val = query(cache, "select p from t where p = 1000").get(0).get(0).toString();

        assertEquals("1000", val);
    }

    /** */
    private void createIndex(IgniteCache<Integer, Integer> cache, boolean multicolumn) {
        query(cache, "create index t_idx on t (p" + (multicolumn ? ", f)" : ")"));
    }

    /** */
    private List<List<?>> query(IgniteCache<Integer, Integer> cache, String qry) {
        return cache.query(new SqlFieldsQuery(qry)).getAll();
    }

    /** */
    private List<List<?>> query(IgniteCache<Integer, Integer> cache, String qry, Object... args) {
        return cache.query(new SqlFieldsQuery(qry).setArgs(args)).getAll();
    }

    /**
     * Tests case when long index deletion operation happens. Checkpoint should run in the middle of index deletion
     * operation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLongIndexDeletion() throws Exception {
        testLongIndexDeletion(false, false, false, false);
    }

    @Test
    public void testLongMulticolumnIndexDeletion() throws Exception {
        testLongIndexDeletion(false, false, false, true);
    }

    /**
     * Tests case when long index deletion operation happens. Checkpoint should run in the middle of index deletion
     * operation. After checkpoint node should restart without fully deleted index tree.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLongIndexDeletionWithRestart() throws Exception {
        testLongIndexDeletion(true, false, false, false);
    }

    /**
     * Tests case when long index deletion operation happens. Checkpoint should run in the middle of index deletion
     * operation. After deletion start one more node should be included in topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLongIndexDeletionWithRebalance() throws Exception {
        testLongIndexDeletion(false, true, false, false);
    }

    @Test
    public void testLongIndexDeletionWithMultipleNodes() throws Exception {
        testLongIndexDeletion(true, false, true, false);
    }
}

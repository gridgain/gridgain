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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MessageOrderLogListener;
import org.apache.ignite.testframework.junits.SystemPropertiesList;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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

            if (blockDestroy.get())
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
    private void testLongIndexDeletion(boolean restart) throws Exception {
        Ignite ignite = startGrids(1);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.query(new SqlFieldsQuery("create table t (id integer primary key, p integer)")).getAll();
        cache.query(new SqlFieldsQuery("create index t_idx on t (p)")).getAll();

        for (int i = 0; i < 20_000; i++)
            cache.query(new SqlFieldsQuery("insert into t (id, p) values (" + i + ", " + i + ")")).getAll();

        forceCheckpoint();

        // Ensure that index is used before drop.
        String plan = cache.query(new SqlFieldsQuery("explain select id, p from t where p = 0")).getAll()
            .get(0).get(0).toString();

        assertTrue(plan, plan.toUpperCase().contains("T_IDX"));

        final IgniteCache<Integer, Integer> finalCache = cache;

        Thread dropIndex = new Thread(() -> {
            testLog.info("Starting index drop");

            finalCache.query(new SqlFieldsQuery("drop index t_idx")).getAll();

            testLog.info("Index drop completed");
        });

        indexDropProcessListener.reset();

        dropIndex.start();

        // Waiting for some modified pages
        doSleep(500);

        forceCheckpoint();

        if (restart) {
            blockDestroy.set(true);

            stopGrid(0, true);

            blockDestroy.set(false);

            ignite = startGrids(1);

            ignite.cluster().active(true);

            cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

            //cache.query(new SqlFieldsQuery("drop index t_idx")).getAll();

            for (int i = 0; i < 20_000; i += 10)
                cache.query(new SqlFieldsQuery("select id, p from t where p = " + i)).getAll();
        }

        dropIndex.join();

        // Ensure that index is not used after it was dropped.
        plan = cache.query(new SqlFieldsQuery("explain select id, p from t where p = 0")).getAll()
            .get(0).get(0).toString();

        assertFalse(plan, plan.toUpperCase().contains("T_IDX"));

        assertFalse(blockedSystemCriticalThreadLsnr.check());

        assertTrue(indexDropProcessListener.check());
    }

    /**
     * Tests case when long index deletion operation happens. Checkpoint should run in the middle of index deletion
     * operation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLongIndexDeletion() throws Exception {
        testLongIndexDeletion(false);
    }

    /**
     * Tests case when long index deletion operation happens. Checkpoint should run in the middle of index deletion
     * operation. After checkpoint node should restart without fully deleted index tree.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLongIndexDeletionWithRestart() throws Exception {
        testLongIndexDeletion(true);
    }

    @Test
    public void testLongIndexDeletion1() throws Exception {
        Ignite ignite = startGrids(1);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.query(new SqlFieldsQuery("create table t (id integer primary key, p integer)")).getAll();
        cache.query(new SqlFieldsQuery("create index t_idx on t (p)")).getAll();

        for (int i = 0; i < 2000; i++)
            cache.query(new SqlFieldsQuery("insert into t (id, p) values (" + i + ", " + i + ")")).getAll();

        cache.query(new SqlFieldsQuery("select count(*) from t")).getAll();
    }
}

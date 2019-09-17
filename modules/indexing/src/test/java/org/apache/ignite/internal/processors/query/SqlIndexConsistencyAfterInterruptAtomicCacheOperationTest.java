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

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class SqlIndexConsistencyAfterInterruptAtomicCacheOperationTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEYS = 1000;

    /**
     * Test's parameters.
     */
    @Parameterized.Parameters(name = "atomicity={0}, nodesCount={1}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
            new Object[] {CacheAtomicityMode.ATOMIC,            1},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL,     1},
            new Object[] {CacheAtomicityMode.ATOMIC,            2},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL,     2}
        );
    }

    /** Enable persistence for the test. */
    @Parameterized.Parameter(0)
    public CacheAtomicityMode atomicity;

    /** Enable persistence for the test. */
    @Parameterized.Parameter(1)
    public int nodesCnt;

    /** Test cache. */
    private IgniteCache<Object, Object> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(nodesCnt);

        cache = grid(0).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicity)
            .setIndexedTypes(Integer.class, Integer.class));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testCachePut() throws Exception {
        interruptOperation(() -> cache.put(1, 1));

        waitEquals(() -> cache.size(), () -> cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testCachePutAll() throws Exception {
        final Map<Integer, Integer> batch = new HashMap<>();

        for (int i = 0; i < KEYS; ++i)
            batch.put(i, i);

        interruptOperation(() -> cache.putAll(batch));

        waitEquals(() -> cache.size(), () -> cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testCacheRemove() throws Exception {
        cache.put(1, 1);

        interruptOperation(() -> cache.remove(1));

        waitEquals(() -> cache.size(), () -> cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testCacheRemoveAll() throws Exception {
        final Map<Integer, Integer> batch = new HashMap<>();

        for (int i = 0; i < KEYS; ++i)
            batch.put(i, i);

        cache.putAll(batch);

        interruptOperation(() -> cache.removeAll(batch.keySet()));

        waitEquals(() -> cache.size(), () -> cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testSqlInsert() throws Exception {
        interruptOperation(() -> cache.query(new SqlFieldsQuery("INSERT INTO Integer (_KEY, _VAL) VALUES (1, 1)")));

        waitEquals(() -> cache.size(), () -> cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testSqlDelete() throws Exception {
        final Map<Integer, Integer> batch = new HashMap<>();

        for (int i = 0; i < KEYS; ++i)
            batch.put(i, i);

        cache.putAll(batch);

        interruptOperation(() -> cache.query(new SqlFieldsQuery("DELETE FROM Integer WHERE _KEY > " + KEYS / 2)));

        waitEquals(() -> cache.size(), () -> cache.query(new SqlFieldsQuery("select * from Integer")).getAll().size());
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testSqlUpdate() throws Exception {
        final Map<Integer, Integer> batch = new HashMap<>();

        for (int i = 0; i < KEYS; ++i)
            batch.put(i, i);

        cache.putAll(batch);

        interruptOperation(() -> cache.query(new SqlFieldsQuery("UPDATE Integer SET _VAL = VAL + 1" + KEYS / 2)));

        final List<List<?>> res = cache.query(new SqlFieldsQuery("select _KEY, _VAL from Integer")).getAll();

        waitEquals(() -> cache.size(), res::size);

        for (List<?> r : res) {
            Integer k = (Integer)r.get(0);

            assertEquals(cache.get(k), r.get(0));
        }
    }

    /**
     * @param r Test operation to run and interrupt.
     * @throws Exception On error.
     */
    private void interruptOperation(Runnable r) throws Exception {
        final boolean[] interrupted = {false};

        Thread t = new Thread(() -> {
            try {
                r.run();
            }
            finally {
                interrupted[0] = Thread.currentThread().isInterrupted();
            }
        });

        t.start();
        t.interrupt();
        t.join();

        assertTrue(interrupted[0]);
    }

    /**
     * Check that result of two operations equal.
     * @param op0 One test operation.
     * @param op1 Other test operation.
     * @throws IgniteInterruptedCheckedException On error.
     */
    private void waitEquals(Supplier<Integer> op0, Supplier<Integer> op1) throws IgniteInterruptedCheckedException {
        if (!GridTestUtils.waitForCondition
            (() -> op0.get().equals(op1.get()),
                2000))
            fail("Not equals:\nExpected: " + op0.get() +"\nActual: " + op1.get());
    }
}
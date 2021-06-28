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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 * Test partitions consistency for atomic caches trying to reuse tx scenarios as much as possible.
 */
@RunWith(Parameterized.class)
public class AtomicPartitionCounterStateConsistencyTest extends TxPartitionCounterStateConsistencyTest {
    /** */
    @Parameterized.Parameter(0)
    public int x;

    /** */
    @Parameterized.Parameters(
        name = "x = {0}")
    public static List<Integer> parameters() {
        ArrayList<Integer> params = new ArrayList<>();

        for (int i = 0; i < 50; i++) {
            params.add(i);
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration<Object, Object> cacheConfiguration(String name) {
        return super.cacheConfiguration(name).setAtomicityMode(ATOMIC);
    }

    /**
     * Test primary-backup partitions consistency while restarting primary node under load.
     */
    @Test
    public void testPartitionConsistencyWithPrimaryRestart() throws Exception {
        backups = 2;

        Ignite prim = startGridsMultiThreaded(SERVER_NODES);

        IgniteEx client = startGrid("client");

        IgniteCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        List<Integer> primaryKeys = primaryKeys(prim.cache(DEFAULT_CACHE_NAME), 10_000);

        long stop = U.currentTimeMillis() + GridTestUtils.SF.applyLB(2 * 60_000, 30_000);

        Random r = new Random();

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            while (U.currentTimeMillis() < stop) {
                doSleep(GridTestUtils.SF.applyLB(30_000, 15_000));

                stopGrid(true, prim.name());

                try {
                    awaitPartitionMapExchange();

                    startGrid(prim.name());

                    awaitPartitionMapExchange();

                    doSleep(GridTestUtils.SF.applyLB(5_000, 2_000));
                }
                catch (Exception e) {
                    fail(X.getFullStackTrace(e));
                }
            }
        }, 1, "node-restarter");

        doRandomUpdates(r, client, primaryKeys, cache, () -> U.currentTimeMillis() >= stop).get();
        fut.get();

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<?> doRandomUpdates(Random r, Ignite near, List<Integer> primaryKeys,
        IgniteCache<Object, Object> cache, BooleanSupplier stopClo) throws Exception {
        LongAdder puts = new LongAdder();
        LongAdder removes = new LongAdder();

        final int max = 100;

        return multithreadedAsync(() -> {
            while (!stopClo.getAsBoolean()) {
                int rangeStart = r.nextInt(primaryKeys.size() - max);
                int range = 5 + r.nextInt(max - 5);

                List<Integer> keys = primaryKeys.subList(rangeStart, rangeStart + range);

                final boolean batch = r.nextBoolean();

                try {
                    List<Integer> insertedKeys = new ArrayList<>();
                    List<Integer> rmvKeys = new ArrayList<>();

                    for (Integer key : keys) {
                        if (!batch)
                            cache.put(key, key);

                        insertedKeys.add(key);

                        puts.increment();

                        boolean rmv = r.nextFloat() < 0.5;
                        if (rmv) {
                            key = insertedKeys.get(r.nextInt(insertedKeys.size()));

                            if (!batch)
                                cache.remove(key);
                            else
                                rmvKeys.add(key);

                            removes.increment();
                        }
                    }

                    if (batch) {
                        cache.putAll(insertedKeys.stream().collect(toMap(k -> k, v -> v, (k, v) -> v, LinkedHashMap::new)));

                        Collections.sort(rmvKeys);

                        cache.removeAll(new LinkedHashSet<>(rmvKeys));
                    }
                }
                catch (Exception e) {
                    assertTrue(X.getFullStackTrace(e), X.hasCause(e, ClusterTopologyException.class) ||
                        X.hasCause(e, ClusterTopologyCheckedException.class) ||
                        X.hasCause(e, CacheInvalidStateException.class));
                }
            }

            log.info("ATOMIC: puts=" + puts.sum() + ", removes=" + removes.sum() + ", size=" + cache.size());

        }, Runtime.getRuntime().availableProcessors() * 2, "tx-update-thread");
    }
}

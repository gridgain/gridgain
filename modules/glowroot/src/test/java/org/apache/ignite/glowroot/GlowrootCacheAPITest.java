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

package org.apache.ignite.glowroot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 */
public class GlowrootCacheAPITest extends GridCommonAbstractTest {
    /** */
    private static final String DEFAULT_CACHE_NAME_2 = DEFAULT_CACHE_NAME + "2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME).
            setAtomicityMode(TRANSACTIONAL).
            setWriteSynchronizationMode(FULL_SYNC);

        ccfg.setIndexedTypes(Integer.class, Integer.class);

        CacheConfiguration<Integer, Integer> ccfg2 = new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME_2).
            setAtomicityMode(TRANSACTIONAL).
            setWriteSynchronizationMode(FULL_SYNC);

        ccfg.setIndexedTypes(Long.class, Long.class);

        cfg.setCacheConfiguration(ccfg, ccfg2);

        cfg.setClientMode(igniteInstanceName.equals("client"));

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testBasicApi() throws Exception {
        try {
            IgniteEx grid = startGrids(2);

            awaitPartitionMapExchange();

            IgniteEx client = startGrid("client");

            IgniteCache<Integer, Integer> cache = client.cache(DEFAULT_CACHE_NAME);
            IgniteCache<Object, Object> cache2 = client.cache(DEFAULT_CACHE_NAME_2);

            for (int i = 0; i < 20_000; i++) {
                // Start complex tx:
                try (Transaction tx = client.transactions().withLabel("myComplexTx" + i).txStart()) {
                    int key = 10;

                    Integer val = cache.get(key);

                    if (val == null)
                        val = 0;

                    cache.put(key, val + 1);

                    Map<Integer, Integer> m = new TreeMap<>();
                    m.put(20, 200);
                    m.put(30, 300);
                    m.put(40, 400);

                    cache2.putAll(m);

                    cache2.put(13, "testStr");

                    SqlFieldsQuery qry = new SqlFieldsQuery("select _KEY from Integer where _KEY=?");
                    qry.setArgs(2);

                    try (FieldsQueryCursor<List<?>> query = cache.query(qry)) {
                        for (List<?> objects : query) {
                            // No-op.
                        }
                    }

                    client.compute().run(new MyClosure());

                    client.compute().execute(new MyTask(), "testArg");

                    tx.commit();
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    private static final class MyClosure implements IgniteRunnable {
        @Override public void run() {
            // No-op.
        }
    }

    private static final class MyTask extends ComputeTaskAdapter<String, Long> {
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            String arg) throws IgniteException {

            Map<MyJob, ClusterNode> map = new HashMap<>();

            for (ClusterNode node : subgrid)
                map.put(new MyJob(), node);

            return map;
        }

        @Override public Long reduce(List<ComputeJobResult> results) throws IgniteException {
            return results.stream().map(new Function<ComputeJobResult, Long>() {
                @Override public Long apply(ComputeJobResult result) {
                    return (Long)result.getData();
                }
            }).reduce(new BinaryOperator<Long>() {
                @Override public Long apply(Long aLong, Long aLong2) {
                    return aLong + aLong2;
                }
            }).get();
        }

        private static class MyJob extends ComputeJobAdapter {
            @Override public Long execute() throws IgniteException {
                return 1L;
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 600_000L;
    }
}

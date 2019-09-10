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
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * { "name": "GridGain CE Plugin", "id": "gridgain_ce", "aspects": [ "org.apache.ignite.glowroot.CacheAPIAspect" ] }
 *
 * { "name": "Ignite Plugin", "id": "ignite", "instrumentation": [ { "captureKind": "transaction", "transactionType":
 * "Ignite", "transactionNameTemplate": "IgniteCommit", "traceEntryMessageTemplate": "{{this}}", "timerName":
 * "IgniteCommit", "className": "org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl",
 * "methodName": "commit", "methodParameterTypes": [] } ] }
 */
public class GlowrootCacheAPITest extends GridCommonAbstractTest {
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME).
            setAtomicityMode(TRANSACTIONAL).
            setWriteSynchronizationMode(FULL_SYNC);

        ccfg.setIndexedTypes(Integer.class, Integer.class);

        cfg.setCacheConfiguration(ccfg);

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

            IgniteEx client = startGrid("client");

            IgniteCache<Object, Object> cache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).
                setAtomicityMode(TRANSACTIONAL).setWriteSynchronizationMode(FULL_SYNC));

            long stop = System.currentTimeMillis() + 600_000L;

            Random r = new Random();

            AtomicInteger i = new AtomicInteger();

            multithreadedAsync(new Runnable() {
                @Override public void run() {
                    int idx = i.getAndIncrement();

                    while (U.currentTimeMillis() < stop) {
                        try (Transaction tx = client.transactions().withLabel("test" + idx).txStart()) {
                            switch (r.nextInt(4)) {
                                case 0:
                                    cache.put(r.nextInt(100), r.nextInt(100));

                                    break;
                                case 1:
                                    Map<Integer, Integer> m = new TreeMap<>();
                                    m.put(r.nextInt(100), r.nextInt(100));
                                    m.put(r.nextInt(100), r.nextInt(100));
                                    m.put(r.nextInt(100), r.nextInt(100));

                                    cache.putAll(m);

                                    break;
                                case 2:
                                    SqlFieldsQuery qry = new SqlFieldsQuery("select _KEY from Integer where _KEY=?");
                                    qry.setArgs(r.nextInt(100));

                                    cache.query(qry).getAll();

                                    break;
                                case 3:
                                    client.compute().run(new MyTask());

                                    break;
                            }

                            tx.commit();
                        }
                    }

                }
            }, 2, "tx-put-thread").get();
        }
        finally {
            stopAllGrids();
        }
    }

    private static final class MyTask implements IgniteRunnable{
        @Override public void run() {

        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 600_000L;
    }
}

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

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

/**
 * { "name": "GridGain CE Plugin", "id": "gridgain_ce", "aspects": [ "org.apache.ignite.glowroot.CacheAPIAspect" ] }
 *
 * { "name": "Ignite Plugin", "id": "ignite", "instrumentation": [ { "captureKind": "transaction", "transactionType":
 * "Ignite", "transactionNameTemplate": "IgniteCommit", "traceEntryMessageTemplate": "{{this}}", "timerName":
 * "IgniteCommit", "className": "org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl",
 * "methodName": "commit", "methodParameterTypes": [] } ] }
 */
public class GlowrootCacheAPITest extends GridCommonAbstractTest {
    /**
     *
     */
    @Test
    public void testBasicApi() throws Exception {
        try {
            IgniteEx grid = startGrid(0);

            IgniteCache<Object, Object> cache = grid.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).
                setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

            long stop = System.currentTimeMillis() + 600_000L;

            Random r = new Random();

            AtomicInteger i = new AtomicInteger();

            multithreadedAsync(new Runnable() {
                @Override public void run() {
                    int idx = i.getAndIncrement();

                    while (U.currentTimeMillis() < stop) {
                        try (Transaction tx = grid.transactions().withLabel("test" + idx).txStart()) {
                            cache.put(r.nextInt(100), r.nextInt(100));

                            tx.commit();
                        }
                    }

                }
            }, 1, "tx-put-thread").get();
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 600_000L;
    }
}

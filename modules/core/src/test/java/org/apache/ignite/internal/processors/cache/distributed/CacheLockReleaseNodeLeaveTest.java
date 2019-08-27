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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class CacheLockReleaseNodeLeaveTest extends GridCommonAbstractTest {
    /** */
    private static final String REPLICATED_TEST_CACHE = "REPLICATED_TEST_CACHE";

    /** {@inheritDoc} */
    @Override public void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.ENTRY_LOCK);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration ccfg1 = new CacheConfiguration(REPLICATED_TEST_CACHE)
            .setCacheMode(REPLICATED)
            .setAtomicityMode(TRANSACTIONAL)
            .setReadFromBackup(false);

        cfg.setCacheConfiguration(ccfg, ccfg1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxLockRelease() throws Exception {
        startGrids(2);

        final Ignite ignite0 = ignite(0);
        final Ignite ignite1 = ignite(1);

        final Integer key = primaryKey(ignite1.cache(DEFAULT_CACHE_NAME));

        IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

                ignite0.cache(DEFAULT_CACHE_NAME).get(key);

                return null;
            }
        }, "lock-thread1");

        fut1.get();

        IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Transaction tx = ignite1.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    log.info("Start tx lock.");

                    ignite1.cache(DEFAULT_CACHE_NAME).get(key);

                    log.info("Tx locked key.");

                    tx.commit();
                }

                return null;
            }
        }, "lock-thread2");

        U.sleep(1000);

        log.info("Stop node.");

        ignite0.close();

        fut2.get(5, SECONDS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxLockRelease2() throws Exception {
        final Ignite ignite0 = startGrid(0);

        Ignite ignite1 = startGrid(1);

        IgniteCache cache = ignite1.cache(DEFAULT_CACHE_NAME);
        ignite1.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);
        cache.get(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(2);

                return null;
            }
        });

        final AffinityTopologyVersion topVer = new AffinityTopologyVersion(2, 0);

        // Wait when affinity change exchange start.
        boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                GridDhtTopologyFuture topFut =
                    ((IgniteKernal)ignite0).context().cache().context().exchange().lastTopologyFuture();

                return topFut != null && topVer.compareTo(topFut.initialVersion()) < 0;
            }
        }, 10_000);

        assertTrue(wait);

        assertFalse(fut.isDone());

        ignite1.close();

        fut.get(10_000);

        Ignite ignite2 = ignite(2);

        cache = ignite2.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ignite2.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.get(1);

            tx.commit();
        }
    }
}

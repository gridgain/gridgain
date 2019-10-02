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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.util.IgniteUtils.compact;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;

/**
 * Tests verifying that {@link IgniteTxAdapter#logTxFinishErrorSafe}
 * runs without errors.
 */
public class SafeLogTxFinishErrorTest extends GridCommonAbstractTest {
    /** Logger for listen log messages. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, super.log);

    /** Flag to remove the FailureHandler when creating configuration for node. */
    private boolean removeFailureHandler;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setAtomicityMode(TRANSACTIONAL)
        );

        if (removeFailureHandler)
            cfg.setFailureHandler(null);

        return cfg;
    }

    /**
     * Checking that {@link IgniteTxAdapter#logTxFinishErrorSafe} is performed
     * without errors with FailureHandler.
     *
     * @throws Exception If any error occurs.
     */
    @Test
    public void testSafeLogTxFinishErrorWithFailureHandler() throws Exception {
        checkSafeLogTxFinishError();
    }

    /**
     * Checking that {@link IgniteTxAdapter#logTxFinishErrorSafe} is performed
     * without errors without FailureHandler.
     *
     * @throws Exception If any error occurs.
     */
    @Test
    public void testSafeLogTxFinishErrorWithoutFailureHandler()throws Exception {
        removeFailureHandler = true;

        checkSafeLogTxFinishError();
    }

    /**
     * Checking that {@link IgniteTxAdapter#logTxFinishErrorSafe} is performed
     * without errors.
     *
     * @throws Exception If any error occurs.
     */
    private void checkSafeLogTxFinishError() throws Exception {
        IgniteEx igniteEx = startGrid(0);

        try (Transaction transaction = igniteEx.transactions().txStart()) {
            IgniteCache<Object, Object> cache = igniteEx.cache(DEFAULT_CACHE_NAME);

            cache.put(1, 1);

            Collection<IgniteInternalTx> activeTxs = igniteEx.context().cache().context().tm().activeTransactions();

            assertEquals(1, activeTxs.size());

            GridNearTxLocal activeTx = (GridNearTxLocal)activeTxs.iterator().next();

            AtomicBoolean containsFailedCompletingTxInLog = new AtomicBoolean();
            AtomicLong curTimeMillis = new AtomicLong();

            log.registerListener(logStr -> {
                setFieldValue(IgniteUtils.class,"curTimeMillis", curTimeMillis.get());

                if (logStr.equals(toFailedCompletingTxMsg(false, activeTx.toString())))
                    containsFailedCompletingTxInLog.set(true);
            });

            curTimeMillis.set(getFieldValue(IgniteUtils.class, "curTimeMillis"));

            activeTx.logTxFinishErrorSafe(log, false, new RuntimeException("Test"));

            assertTrue(containsFailedCompletingTxInLog.get());
        }
    }

    /**
     * Generates a transaction completion failure message.
     */
    private String toFailedCompletingTxMsg(boolean commit, String tx) {
        assert nonNull(tx);

        return compact(format("Failed completing the transaction: [commit=%s, tx=%s]", commit, tx));
    }
}

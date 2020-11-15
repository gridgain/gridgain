package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Assert;
import org.junit.Test;

import javax.cache.CacheException;

/**
 * Tests check that second operation will be failed if it doesn't pass validation.
 */
public class TransactionValidationTest extends GridCommonAbstractTest  {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void validationOnRemoteNode() throws Exception {
        validationTest(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void validationOnLocalNode() throws Exception {
        validationTest(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void validationTest(boolean distributed) throws Exception {
        IgniteEx txCrd;

        if (distributed) {
            startGrid(0);
            startGrid(1);
            txCrd = startClientGrid(2);
        }
        else
            txCrd = startGrid(0);

        IgniteCache<Object, Object> cache0 = txCrd.createCache(
                new CacheConfiguration<>("cache0")
                        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        );

        IgniteCache<Object, Object> cache1 = txCrd.createCache(
                new CacheConfiguration<>("cache1")
                        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                        .setTopologyValidator(nodes -> false)
        );

        try (Transaction tx = txCrd.transactions().txStart()) {
            cache0.put(1, 1);

            CacheException e = null;

            try {
                cache1.put(1, 1);
            } catch (CacheException e0) {
                e = e0;
            }

            Assert.assertTrue(e.getCause() instanceof CacheInvalidStateException);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }
}

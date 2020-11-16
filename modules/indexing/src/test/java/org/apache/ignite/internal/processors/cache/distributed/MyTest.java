package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;

public class MyTest extends GridCommonAbstractTest {
    @Test
    public void test2() throws Exception {
        IgniteEx srv0 = startGrid(0);

        IgniteCache<Object, Object> cache = srv0.createCache(
                new CacheConfiguration<>("cache1")
                        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                        .setCacheMode(CacheMode.PARTITIONED)
        );

        try (Transaction tx = srv0.transactions().txStart()) {

            cache.put(1, 1);
            cache.put(2, 2);
        }
    }
}

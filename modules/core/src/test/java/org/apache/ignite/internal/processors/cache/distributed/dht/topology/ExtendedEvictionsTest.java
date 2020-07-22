package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

@WithSystemProperty(key = "IGNITE_PRELOAD_RESEND_TIMEOUT", value = "0")
public class ExtendedEvictionsTest extends AbstractPartitionClearingTest {
    /**
     * Stopping the cache during clearing should be no-op, all partitions are expected to be cleared.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheGroupRestartDuringEviction_1() throws Exception {
        testOperationDuringEviction(false, 1, new Runnable() {
            @Override public void run() {
                doSleep(1000); // Wait a bit to give some time for partition state message to process on crd.

                grid(0).cache(DEFAULT_CACHE_NAME).close();
            }
        });
    }

    @Test
    public void testCacheGroupRestartDuringEviction_2() throws Exception {
        testOperationDuringEviction(false, 1, new Runnable() {
            @Override public void run() {
                // No-op.
            }
        });
    }

    public void testNodeStopDuringEviction() {

    }

    public void testCacheStopDuringEvictionMultiCacheGroup() {

    }

//    public void testPartitionMapRefreshAfterAllEvicted() {
//
//    }
//
//    public void testPartitionMapNotSendAfterAllClearedBeforeRebalancing() {
//
//    }
//
//    public void testConcurrentEvictionSamePartition() {
//
//    }
//
//    public void testEvictOneThreadInSysPool() {
//
//    }
}

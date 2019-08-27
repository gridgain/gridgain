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

import java.util.concurrent.CountDownLatch;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVTS_CACHE;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;

/**
 * Test cases for multi-threaded tests.
 */
@SuppressWarnings("LockAcquiredButNotSafelyReleased")
public abstract class GridCacheBasicApiAbstractTest extends GridCommonAbstractTest {
    /** Grid. */
    private Ignite ignite;

    /**
     *
     */
    protected GridCacheBasicApiAbstractTest() {
        super(true /*start grid.*/);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = grid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite = null;
    }

    /**
     *
     * @throws Exception If error occur.
     */
    @Test
    public void testBasicOps() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.ENTRY_LOCK);
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_EVENTS);

        IgniteCache<Integer, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

        CountDownLatch latch = new CountDownLatch(1);

        CacheEventListener lsnr = new CacheEventListener(latch);

        try {
            ignite.events().localListen(lsnr, EVTS_CACHE);

            int key = (int)System.currentTimeMillis();

            assert !cache.containsKey(key);

            cache.put(key, "a");

            info("Start latch wait 1");

            latch.await();

            info("Stop latch wait 1");

            assert cache.containsKey(key);

            latch = new CountDownLatch(2);

            lsnr.latch(latch);

            cache.put(key, "b");
            cache.put(key, "c");

            info("Start latch wait 2");

            latch.await();

            info("Stop latch wait 2");

            assert cache.containsKey(key);

            latch = new CountDownLatch(1);

            lsnr.latch(latch);

            cache.remove(key);

            info("Start latch wait 3");

            latch.await();

            info("Stop latch wait 3");

            assert !cache.containsKey(key);
        }
        finally {
            ignite.events().stopLocalListen(lsnr, EVTS_CACHE);
        }
    }

    /**
     *
     */
    @Test
    public void testGetPutRemove() {
        IgniteCache<Integer, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

        int key = (int)System.currentTimeMillis();

        assert cache.get(key) == null;
        assert cache.getAndPut(key, "1") == null;

        String val = cache.get(key);

        assert val != null;
        assert "1".equals(val);

        val = cache.getAndRemove(key);

        assert val != null;
        assert "1".equals(val);
        assert cache.get(key) == null;
    }

    /**
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testPutWithExpiration() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.ENTRY_LOCK);
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EXPIRATION);
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_EVENTS);

        IgniteCache<Integer, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

        CacheEventListener lsnr = new CacheEventListener(new CountDownLatch(1));

        ignite.events().localListen(lsnr, EVTS_CACHE);

        ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, 200L));

        try {
            int key = (int)System.currentTimeMillis();

            cache.withExpiryPolicy(expiry).put(key, "val");

            assert cache.get(key) != null;

            cache.withExpiryPolicy(expiry).put(key, "val");

            Thread.sleep(500);

            assert cache.get(key) == null;
        }
        finally {
            ignite.events().stopLocalListen(lsnr, EVTS_CACHE);
        }
    }

    /**
     * Event listener.
     */
    private class CacheEventListener implements IgnitePredicate<Event> {
        /** Wait latch. */
        private CountDownLatch latch;

        /** Event types. */
        private int[] types;

        /**
         * @param latch Wait latch.
         * @param types Event types.
         */
        CacheEventListener(CountDownLatch latch, int... types) {
            this.latch = latch;
            this.types = types;

            if (F.isEmpty(types))
                this.types = new int[] { EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED };
        }

        /**
         * @param latch New latch.
         */
        void latch(CountDownLatch latch) {
            this.latch = latch;
        }

        /**
         * Waits for latch.
         *
         * @throws InterruptedException If got interrupted.
         */
        void await() throws InterruptedException {
            latch.await();
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            info("Grid cache event: " + evt);

            if (U.containsIntArray(types, evt.type()))
                latch.countDown();

            return true;
        }
    }
}

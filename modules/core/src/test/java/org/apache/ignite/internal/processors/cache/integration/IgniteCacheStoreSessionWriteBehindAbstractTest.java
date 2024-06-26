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

package org.apache.ignite.internal.processors.cache.integration;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public abstract class IgniteCacheStoreSessionWriteBehindAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private static final String CACHE_NAME1 = "cache1";

    /** */
    private static volatile CountDownLatch latch;

    /** */
    protected static volatile CountDownLatch entLatch;

    /** */
    private static volatile ExpectedData expData;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    @Override @SuppressWarnings("unchecked")
    protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg0 = super.cacheConfiguration(igniteInstanceName);

        ccfg0.setReadThrough(true);
        ccfg0.setWriteThrough(true);
        ccfg0.setWriteBehindBatchSize(10);
        ccfg0.setWriteBehindFlushSize(10);
        ccfg0.setWriteBehindFlushFrequency(600);
        ccfg0.setWriteBehindEnabled(true);

        ccfg0.setCacheStoreFactory(singletonFactory(new TestStore()));

        return ccfg0;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        assert cfg.getCacheConfiguration().length == 1;

        CacheConfiguration ccfg0 = cacheConfiguration(igniteInstanceName);

        ccfg0.setName(DEFAULT_CACHE_NAME);

        CacheConfiguration ccfg1 = cacheConfiguration(igniteInstanceName);

        ccfg1.setName(CACHE_NAME1);

        cfg.setCacheConfiguration(ccfg0, ccfg1);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSession() throws Exception {
        testCache(DEFAULT_CACHE_NAME);

        testCache(CACHE_NAME1);
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void testCache(String cacheName) throws Exception {
        IgniteCache<Integer, Integer> cache = ignite(0).cache(cacheName);

        try {
            latch = new CountDownLatch(2);
            entLatch = new CountDownLatch(11);

            expData = new ExpectedData("writeAll", cacheName);

            for (int i = 0; i < 11; i++)
                cache.put(i, i);

            assertTrue(latch.await(10_000, TimeUnit.MILLISECONDS));

            assertTrue(entLatch.await(10_000,TimeUnit.MILLISECONDS));
        }
        finally {
            latch = null;
            entLatch = null;
        }

        try {
            latch = new CountDownLatch(2);
            entLatch = new CountDownLatch(11);

            expData = new ExpectedData("deleteAll", cacheName);

            for (int i = 0; i < 11; i++)
                cache.remove(i);

            assertTrue(latch.await(10_000, TimeUnit.MILLISECONDS));

            assertTrue(entLatch.await(10_000,TimeUnit.MILLISECONDS));
        }
        finally {
            latch = null;
            entLatch = null;
        }
    }

    /**
     *
     */
    protected class TestStore implements CacheStore<Object, Object> {

        /** Auto-injected store session. */
        @CacheStoreSessionResource
        private CacheStoreSession ses;

        /** */
        @IgniteInstanceResource
        protected Ignite ignite;

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, @Nullable Object... args) {
            fail();
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Map<Object, Object> loadAll(Iterable<?> keys) throws CacheLoaderException {
            fail();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
            fail();
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<?, ?>> entries) throws CacheWriterException {
            log.info("writeAll: " + entries);

            assertTrue("Unexpected entries: " + entries, entries.size() == 10 || entries.size() == 1);

            checkSession("writeAll");

            for (int i = 0; i < entries.size(); i++)
                entLatch.countDown();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            fail();
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            log.info("deleteAll: " + keys);

            assertTrue("Unexpected keys: " + keys, keys.size() == 10 || keys.size() == 1);

            checkSession("deleteAll");

            for (int i = 0; i < keys.size(); i++)
                entLatch.countDown();
        }

        /**
         * @return Store session.
         */
        private CacheStoreSession session() {
            return ses;
        }

        /**
         * @param mtd Called stored method.
         */
        protected void checkSession(String mtd) {
            assertNotNull(ignite);

            CacheStoreSession ses = session();

            assertNotNull(ses);

            log.info("Cache: " + ses.cacheName());

            assertFalse(ses.isWithinTransaction());

            assertNull(ses.transaction());

            assertNotNull(expData);

            assertEquals(mtd, expData.expMtd);

            assertEquals(expData.expCacheName, ses.cacheName());

            assertNotNull(ses.properties());

            ses.properties().put(1, "test");

            assertEquals("test", ses.properties().get(1));

            latch.countDown();
        }
    }

    /**
     *
     */
    static class ExpectedData {
        /** */
        private final String expMtd;

        /** */
        private final String expCacheName;

        /**
         * @param expMtd Expected method.
         * @param expCacheName Expected cache name.
         */
        public ExpectedData(String expMtd, String expCacheName) {
            this.expMtd = expMtd;
            this.expCacheName = expCacheName;
        }
    }
}

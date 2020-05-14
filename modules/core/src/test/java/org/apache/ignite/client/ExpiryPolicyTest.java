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

package org.apache.ignite.client;

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Thin client expiry policy tests.
 *
 * Before every test:
 * 1. Start cluster node;
 * 2. Start thin client connected to that node;
 * 3. Create cache using client without static expiry policy configured;
 *
 * After every test:
 * 1. Stop client;
 * 2. Stop server node;
 */
public class ExpiryPolicyTest extends GridCommonAbstractTest {
    /** Cluster node. */
    Ignite server;

    /** Client. */
    IgniteClient client;

    /** Basic transactional cache with no static expiry policy configured. */
    ClientCache<Integer, Object> dfltCache;

    /** Expiry Policy TTL */
    static final long ttl = 600L;

    /** Timeout to wait to make sure that value is expired. */
    static final long timeout = ttl * 3;

    /** Duration to be used in expiry policies. */
    static final Duration ttlDur = new Duration(TimeUnit.MILLISECONDS, ttl);

    /** Per test timeout */
    @Rule
    public Timeout globalTimeout = new Timeout((int) GridTestUtils.DFLT_TEST_TIMEOUT);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        server = Ignition.start(Config.getServerConfiguration());

        client = Ignition.startClient(new ClientConfiguration()
                .setAddresses(Config.SERVER)
                .setSendBufferSize(0)
                .setReceiveBufferSize(0)
        );

        dfltCache = client.createCache(new ClientCacheConfiguration()
                .setName("cache_dynamic")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        );

        dfltCache.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        client.close();
        client = null;

        server.close();
        server = null;
    }

    /**
     * Ctor.
     */
    public ExpiryPolicyTest() {
        super(false);
    }

    /**
     * Test that cache with dynamic CreatedExpiryPolicy created using thin client works as expected.
     *
     * 1. Start cluster node. Start thin client connected to that node;
     * 2. Create cache using client without static expiry policy configured;
     * 3. Create cache facade for cache from step 2 using CreatedExpiryPolicy;
     * 4. Put a new value using cache facade from step 3. Check that value is in cache;
     * 5. Wait for value to expire;
     * 6. Put a new value using cache from step 2. Check that value is in cache;
     * 7. Modify and get value. Make sure that value is not expired;
     */
    @Test
    public void testCreatedExpiryPolicyDynamic() throws Exception {
        final int key = 1;

        ClientCache<Integer, Object> cachePlcCreated = dfltCache.withExpirePolicy(new CreatedExpiryPolicy(ttlDur));

        cachePlcCreated.put(key, 1);
        assertTrue(cachePlcCreated.containsKey(key));

        assertTrue(GridTestUtils.waitForCondition(() -> !cachePlcCreated.containsKey(key), timeout));

        // Update and access key with created expire policy.
        dfltCache.put(key, 1);
        cachePlcCreated.put(key, 2);
        cachePlcCreated.get(key);

        assertFalse(GridTestUtils.waitForCondition(() -> !cachePlcCreated.containsKey(key), timeout));
    }

    /**
     * Test that cache with static CreatedExpiryPolicy created using thin client works as expected.
     *
     * 1. Start cluster node. Start thin client connected to that node;
     * 2. Create cache using client with static CreatedExpiryPolicy configured;
     * 3. Put a new value using cache from step 2. Check that value is in cache;
     * 4. Make sure that value is expired;
     */
    @Test
    public void testCreatedExpiryPolicyStatic() throws Exception {
        final int key = 1;

        ClientCache<Integer, Object> cache = client.createCache(new ClientCacheConfiguration()
                .setName("cache_static")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setExpiryPolicy(new CreatedExpiryPolicy(ttlDur))
        );

        cache.put(key, 1);
        assertTrue(cache.containsKey(key));

        assertTrue(GridTestUtils.waitForCondition(() -> !cache.containsKey(key), timeout));
    }

    /**
     * Test that cache with dynamic ModifiedExpiryPolicy created using thin client works as expected.
     *
     * 1. Start cluster node. Start thin client connected to that node;
     * 2. Create cache using client without static expiry policy configured;
     * 3. Create cache facade for cache from step 2 using ModifiedExpiryPolicy;
     * 4. Put a new value using cache facade from step 3. Make sure that value is not expired;
     * 5. Get value using cache facade from step 3. Make sure that value is not expired;
     * 6. Modify value using cache facade from step 3. Make sure that value is expired;
     */
    @Test
    public void testModifiedExpiryPolicyDynamic() throws Exception {
        checkModifiedExpiryPolicy(dfltCache.withExpirePolicy(new ModifiedExpiryPolicy(ttlDur)));
    }

    /**
     * Test that cache with static ModifiedExpiryPolicy created using thin client works as expected.
     *
     * 1. Start cluster node. Start thin client connected to that node;
     * 2. Create cache using client with static ModifiedExpiryPolicy;
     * 3. Put a new value using cache from step 2. Make sure that value is not expired;
     * 4. Get value using cache from step 2. Make sure that value is not expired;
     * 5. Modify value using cache from step 2. Make sure that value is expired;
     */
    @Test
    public void testModifiedExpiryPolicyStatic() throws Exception {
        ClientCache<Integer, Object> cache = client.createCache(new ClientCacheConfiguration()
                .setName("cache_static")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setExpiryPolicy(new ModifiedExpiryPolicy(ttlDur))
        );

        checkModifiedExpiryPolicy(cache);
    }

    /**
     * Test that cache with dynamic AccessedExpiryPolicy created using thin client works as expected.
     *
     * 1. Start cluster node. Start thin client connected to that node;
     * 2. Create cache using client without static expiry policy configured;
     * 3. Create cache facade for cache from step 2 using AccessedExpiryPolicy;
     * 4. Put a new value using cache facade from step 3. Make sure that value is not expired;
     * 5. Modify value using cache facade from step 3. Make sure that value is not expired;
     * 6. Get value using cache facade from step 3. Make sure that value is expired;
     */
    @Test
    public void testAccessedExpiryPolicyDynamic() throws Exception {
        checkAccessedExpiryPolicy(dfltCache.withExpirePolicy(new AccessedExpiryPolicy(ttlDur)));
    }

    /**
     * Test that cache with static AccessedExpiryPolicy created using thin client works as expected.
     *
     * 1. Start cluster node. Start thin client connected to that node;
     * 2. Create cache using client with static AccessedExpiryPolicy;
     * 3. Put a new value using cache from step 2. Make sure that value is not expired;
     * 5. Modify value using cache from step 2. Make sure that value is not expired;
     * 6. Get value using cache from step 2. Make sure that value is expired;
     */
    @Test
    public void testAccessedExpiryPolicyStatic() throws Exception {
        ClientCache<Integer, Object> cache = client.createCache(new ClientCacheConfiguration()
                .setName("cache_static")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setExpiryPolicy(new AccessedExpiryPolicy(ttlDur))
        );

        checkAccessedExpiryPolicy(cache);
    }

    /**
     * Test that cache with dynamic CreatedExpiryPolicy expiry policy created using thin client works as expected with
     * keep-binary and in transaction.
     *
     * 1. Start cluster node. Start thin client connected to that node;
     * 2. Create cache using client without static expiry policy configured;
     * 3. Create cache facade for cache from step 2 using CreatedExpiryPolicy;
     * 4. Create cache facede for cache from step 3 withKeepBinary();
     * 5. Put a new value using cache facade from step 4 within transaction;
     * 6. Check that cache contains value and returned in binary form when cache from step 4 is used;
     * 7. Make sure that value is expired;
     */
    @Test
    public void testCreatedExpiryPolicyBinaryTransaction() throws Exception {
        final int key = 4;

        ClientCache<Integer, Object> cachePlcCreated = dfltCache.withExpirePolicy(new CreatedExpiryPolicy(ttlDur));
        ClientCache<Integer, Object> binCache = cachePlcCreated.withKeepBinary();

        try (ClientTransaction tx = client.transactions().txStart()) {
            binCache.put(key, new T2<>("test", "test"));

            tx.commit();
        }

        assertTrue(binCache.get(key) instanceof BinaryObject);
        assertFalse(dfltCache.get(key) instanceof BinaryObject);

        assertTrue(GridTestUtils.waitForCondition(() -> !dfltCache.containsKey(key), timeout));
    }

    /**
     * Check that cache with ModifiedExpiryPolicy works as expected.
     * @param cache Cache to use for testing.
     */
    public static void checkModifiedExpiryPolicy(ClientCache<Integer, Object> cache) throws Exception {
        final int key = 2;

        // Access key with modified expire policy.
        cache.put(key, 1);
        assertFalse(GridTestUtils.waitForCondition(() -> !cache.containsKey(key), timeout));

        // Access key with modified expire policy.
        cache.get(key);
        assertFalse(GridTestUtils.waitForCondition(() -> !cache.containsKey(key), timeout));

        // Modify key with modified expire policy.
        cache.put(key, 2);
        assertTrue(GridTestUtils.waitForCondition(() -> !cache.containsKey(key), timeout));
    }

    /**
     * Check that cache with AccessedExpiryPolicy works as expected.
     * @param cache Cache to use for testing.
     */
    public static void checkAccessedExpiryPolicy(ClientCache<Integer, Object> cache) throws Exception {
        final int key = 3;

        // Access key with accessed expire policy.
        cache.put(key, 1);
        assertFalse(GridTestUtils.waitForCondition(() -> !cache.containsKey(key), timeout));

        // Modify key with accessed expire policy.
        cache.put(key, 2);
        assertFalse(GridTestUtils.waitForCondition(() -> !cache.containsKey(key), timeout));

        // Access key with accessed expire policy.
        cache.get(key);
        assertTrue(GridTestUtils.waitForCondition(() -> !cache.containsKey(key), timeout));
    }
}

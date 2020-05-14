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
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.expiry.*;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
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
    }

    /**
     * Create cache with no expiry policy.
     * @param client Client to use.
     * @return Cache instance.
     */
    private static ClientCache<Integer, Object> createCacheNoPolicy(IgniteClient client) {
        return client.createCache(new ClientCacheConfiguration()
                .setName("cache_dynamic")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        );
    }

    /**
     * Create cache with expiry policy.
     * @param client Client to use.
     * @param policy Expiry policy.
     * @return Cache instance.
     */
    private static ClientCache<Integer, Object> createCacheWithPolicy(IgniteClient client, ExpiryPolicy policy) {
        return client.createCache(new ClientCacheConfiguration()
                .setName("cache_static")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setExpiryPolicy(policy)
        );
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
     * 3. Put a new value using cache from step 2. Check that value is in cache;
     * 4. Create cache facade for cache from step 2 using CreatedExpiryPolicy;
     * 5. Modify and get value using cache facade from step 4. Make sure that value is not expired;
     */
    @Test
    public void testCreatedExpiryPolicyDynamicNegative() throws Exception {
        ClientCache<Integer, Object> cache = createCacheNoPolicy(client);
        cache.put(1, 1);
        cache.containsKey(1);

        ClientCache<Integer, Object> cachePlcCreated = cache.withExpirePolicy(new CreatedExpiryPolicy(ttlDur));

        cachePlcCreated.put(1, 0);
        cachePlcCreated.get(1);

        assertFalse(waitUntilExpired(cache, 1));
    }

    /**
     * Test that cache with dynamic CreatedExpiryPolicy created using thin client works as expected.
     *
     * 1. Start cluster node. Start thin client connected to that node;
     * 2. Create cache using client without static expiry policy configured;
     * 3. Create cache facade for cache from step 2 using CreatedExpiryPolicy;
     * 4. Put a new value using cache facade from step 3. Check that value is in cache;
     * 5. Wait for value to expire; Make sure that update does not prevent value expiration;
     */
    @Test
    public void testCreatedExpiryPolicyDynamicPositive() throws Exception {
        ClientCache<Integer, Object> cache = createCacheNoPolicy(client);
        ClientCache<Integer, Object> cachePlcCreated = cache.withExpirePolicy(new CreatedExpiryPolicy(ttlDur));

        checkCreatedExpiryPolicyPositive(cachePlcCreated);
    }

    /**
     * Test that cache with static CreatedExpiryPolicy created using thin client works as expected.
     *
     * 1. Start cluster node. Start thin client connected to that node;
     * 2. Create cache using client with static CreatedExpiryPolicy configured;
     * 3. Put a new value using cache from step 2. Check that value is in cache;
     * 5. Wait for value to expire; Make sure that update does not prevent value expiration;
     */
    @Test
    public void testCreatedExpiryPolicyStaticPositive() throws Exception {
        ClientCache<Integer, Object> cachePlcCreated = createCacheWithPolicy(client, new CreatedExpiryPolicy(ttlDur));

        checkCreatedExpiryPolicyPositive(cachePlcCreated);
    }

    /**
     * Test that cache with dynamic ModifiedExpiryPolicy created using thin client works as expected.
     *
     * 1. Start cluster node. Start thin client connected to that node;
     * 2. Create cache using client without static expiry policy configured;
     * 3. Put a new value using cache from step 2. Make sure that value is in cache;
     * 4. Create cache facade for cache from step 2 using ModifiedExpiryPolicy;
     * 5. Get value using cache facade from step 4. Make sure that value is not expired;
     * 6. Make sure that value is not expired if constantly updated;
     * 7. Stop updating value. Make sure that value is expired;
     */
    @Test
    public void testModifiedExpiryPolicyDynamicNegative() throws Exception {
        ClientCache<Integer, Object> cache = createCacheNoPolicy(client);
        cache.put(1, 1);
        cache.containsKey(1);

        ClientCache<Integer, Object> cachePlcUpdated = cache.withExpirePolicy(new ModifiedExpiryPolicy(ttlDur));

        cachePlcUpdated.get(1);
        assertFalse(waitUntilExpired(cachePlcUpdated, 1));

        assertFalse(waitUntilExpiredWhileUpdated(cachePlcUpdated, 1));

        assertTrue(waitUntilExpired(cachePlcUpdated, 1));
    }

    /**
     * Test that cache with dynamic ModifiedExpiryPolicy created using thin client works as expected.
     *
     * 1. Start cluster node. Start thin client connected to that node;
     * 2. Create cache using client without static expiry policy configured;
     * 3. Create cache facade for cache from step 2 using ModifiedExpiryPolicy;
     * 4. Put a new value using cache facade from step 3.
     * 5. Make sure that value is not expired if constantly updated;
     * 6. Stop updating value. Make sure that value is expired;
     */
    @Test
    public void testModifiedExpiryPolicyDynamicPositive() throws Exception {
        ClientCache<Integer, Object> cache = createCacheNoPolicy(client);
        ClientCache<Integer, Object> cachePlcUpdated = cache.withExpirePolicy(new ModifiedExpiryPolicy(ttlDur));

        checkModifiedExpiryPolicyPositive(cachePlcUpdated);
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
    public void testModifiedExpiryPolicyStaticPositive() throws Exception {
        ClientCache<Integer, Object> cachePlcUpdated = createCacheWithPolicy(client, new ModifiedExpiryPolicy(ttlDur));

        checkModifiedExpiryPolicyPositive(cachePlcUpdated);
    }

    /**
     * Test that cache with dynamic AccessedExpiryPolicy created using thin client works as expected.
     *
     * 1. Start cluster node. Start thin client connected to that node;
     * 2. Create cache using client without static expiry policy configured;
     * 3. Put a new value using cache from step 2. Make sure that value is in cache;
     * 4. Create cache facade for cache from step 2 using AccessedExpiryPolicy;
     * 5. Update value using cache facade from step 4. Make sure that value is not expired;
     * 6. Make sure that value is not expired if constantly accessed;
     * 7. Stop accessing value. Make sure that value is expired;
     */
    @Test
    public void testAccessedExpiryPolicyDynamicNegative() throws Exception {
        ClientCache<Integer, Object> cache = createCacheNoPolicy(client);
        cache.put(1, 1);
        cache.containsKey(1);

        ClientCache<Integer, Object> cachePlcAccessed = cache.withExpirePolicy(new AccessedExpiryPolicy(ttlDur));

        cachePlcAccessed.put(1, 2);
        assertFalse(waitUntilExpired(cachePlcAccessed, 1));

        assertFalse(waitUntilExpiredWhileAccessed(cachePlcAccessed, 1));

        assertTrue(waitUntilExpired(cachePlcAccessed, 1));
    }

    /**
     * Test that cache with dynamic AccessedExpiryPolicy created using thin client works as expected.
     *
     * 1. Start cluster node. Start thin client connected to that node;
     * 2. Create cache using client without static expiry policy configured;
     * 3. Create cache facade for cache from step 2 using AccessedExpiryPolicy;
     * 4. Put a new value using cache facade from step 3.
     * 5. Make sure that value is not expired if constantly accessed using facade from step 3;
     * 6. Stop accessing value. Make sure that value is expired;
     */
    @Test
    public void testAccessedExpiryPolicyDynamicPositive() throws Exception {
        ClientCache<Integer, Object> cache = createCacheNoPolicy(client);
        ClientCache<Integer, Object> cachePlcAccessed = cache.withExpirePolicy(new AccessedExpiryPolicy(ttlDur));

        checkAccessedExpiryPolicy(cachePlcAccessed);
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
        ClientCache<Integer, Object> cachePlcAccessed = createCacheWithPolicy(client, new AccessedExpiryPolicy(ttlDur));

        checkAccessedExpiryPolicy(cachePlcAccessed);
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
        ClientCache<Integer, Object> cache = createCacheNoPolicy(client);
        ClientCache<Integer, Object> cachePlcCreated = cache.withExpirePolicy(new CreatedExpiryPolicy(ttlDur));
        ClientCache<Integer, Object> binCache = cachePlcCreated.withKeepBinary();

        try (ClientTransaction tx = client.transactions().txStart()) {
            binCache.put(4, new T2<>("test", "test"));

            tx.commit();
        }

        assertTrue(binCache.get(4) instanceof BinaryObject);
        assertFalse(cache.get(4) instanceof BinaryObject);

        assertTrue(waitUntilExpired(cachePlcCreated, 4));
    }

    /**
     * Check that cache with CreatedExpiryPolicy works as expected.
     * @param cache Cache to use for testing.
     */
    private static void checkCreatedExpiryPolicyPositive(ClientCache<Integer, Object> cache) throws Exception {
        cache.put(1, 1);
        assertTrue(cache.containsKey(1));

        assertTrue(waitUntilExpiredWhileUpdated(cache, 1));
    }

    /**
     * Check that cache with ModifiedExpiryPolicy works as expected.
     * @param cache Cache to use for testing.
     */
    private static void checkModifiedExpiryPolicyPositive(ClientCache<Integer, Object> cache) throws Exception {
        cache.put(1, 1);
        assertFalse(waitUntilExpiredWhileUpdated(cache, 1));

        assertTrue(waitUntilExpired(cache, 1));
    }

    /**
     * Check that cache with AccessedExpiryPolicy works as expected.
     * @param cache Cache to use for testing.
     */
    private static void checkAccessedExpiryPolicy(ClientCache<Integer, Object> cache) throws Exception {
        cache.put(1, 2);
        assertFalse(waitUntilExpiredWhileAccessed(cache, 1));

        assertTrue(waitUntilExpired(cache, 1));
    }

    /**
     * Wait until value is expired.
     * @param cache Cache.
     * @param key Key.
     * @return {@code true} if value expired and {@code false} otherwise.
     */
    private static boolean waitUntilExpired(ClientCache<Integer, Object> cache, int key)
            throws IgniteInterruptedCheckedException {
        return GridTestUtils.waitForCondition(() -> !cache.containsKey(key), timeout);
    }

    /**
     * Wait until value is expired while being constantly updated.
     * @param cache Cache.
     * @param key Key.
     * @return {@code true} if value expired and {@code false} otherwise.
     */
    private static boolean waitUntilExpiredWhileUpdated(ClientCache<Integer, Object> cache, int key)
            throws IgniteInterruptedCheckedException {
        AtomicInteger cnt = new AtomicInteger(100);

        return GridTestUtils.waitForCondition(() -> {
            cache.replace(key, cnt.getAndIncrement());

            return !cache.containsKey(key);
        }, timeout);
    }

    /**
     * Wait until value is expired while being constantly accessed.
     * @param cache Cache.
     * @param key Key.
     * @return {@code true} if value expired and {@code false} otherwise.
     */
    private static boolean waitUntilExpiredWhileAccessed(ClientCache<Integer, Object> cache, int key)
            throws IgniteInterruptedCheckedException {
        return GridTestUtils.waitForCondition(() -> {
            cache.get(key);

            return !cache.containsKey(key);
        }, timeout);
    }
}

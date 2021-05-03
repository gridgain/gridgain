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


package org.apache.ignite.internal.client.thin;

import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Test connection algorithms with affinity awareness enabled.
 */
public class ThinClientAffinityAwarenessConnectionTest extends ThinClientAbstractAffinityAwarenessTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Test that everything works with a single connection towards the cluster.
     *
     * 1. Start cluster with 3 nodes;
     * 2. Connect client using single node from affinity;
     * 3. Check that all requests go to a single node (Similiar to the old behaviour). Check for all cache operations;
     */
    @Test
    public void testSingleConnection() throws Exception {
        startGrids(3);

        awaitPartitionMapExchange();

        initClient(getClientConfiguration(1), 1);

        ClientCache<Object, Object> cache = client.cache(PART_CACHE_NAME);

        TestTcpClientChannel expCahnnel = channels[1];

        // Warm up topology info
        cache.put(1, 1);
        opsQueue.clear();

        for (int i = 0; i < KEY_CNT; ++i)
            testAllOperations(cache, expCahnnel, i, i * 2);
    }

    /**
     * Test that client do not connect to nodes which are not listed with correct exception.
     *
     * 1. Start cluster with 3 nodes;
     * 2. Connect client using multiple connections to non existing nodes;
     * 3. Check that no connection created with correct exception;
     */
    @Test
    public void testMultipleConnectionsWithNoNodes() throws Exception {
        startGrids(3);

        awaitPartitionMapExchange();

        GridTestUtils.assertThrows(
                null,
                () -> initClient(getClientConfiguration(4, 5, 6), 4, 5, 6),
                ClientConnectionException.class,
                null);
    }

    /**
     * Test that client do not connect to nodes which are not listed with correct exception.
     *
     * 1. Start cluster with 3 nodes;
     * 2. Connect client using multiple connections to nodes;
     * 3. Kill nodes in connection;
     * 4. Check that *cache operation* failed with correct exception;
     */
    @Test
    public void testMultipleConnectionsWithFailNodes() throws Exception {
        startGrids(3);

        awaitPartitionMapExchange();

        try {
            initClient(getClientConfiguration(1, 2), 1, 2);

            ClientCache<Object, Object> cache = client.cache(PART_CACHE_NAME);

            stopAllGrids();

            cache.put(1, 1);

            fail("Must throw exception");
        }
        catch (ClientConnectionException err) {
            assertTrue(err.getMessage(), err.getMessage().contains("Channel is closed"));
        }
    }

    /**
     * Test all operations with the pair of key and value.
     * @param cache Cache to use.
     * @param opCh Channel which expected to transfer operation.
     * @param key Key.
     * @param value Value.
     */
    private void testAllOperations(ClientCache<Object, Object> cache, TestTcpClientChannel opCh, Object key, Object value) {
        cache.put(key, key);

        assertOpOnChannel(opCh, ClientOperation.CACHE_PUT);

        cache.get(key);

        assertOpOnChannel(opCh, ClientOperation.CACHE_GET);

        cache.containsKey(key);

        assertOpOnChannel(opCh, ClientOperation.CACHE_CONTAINS_KEY);

        cache.replace(key, value);

        assertOpOnChannel(opCh, ClientOperation.CACHE_REPLACE);

        cache.replace(key, value, value);

        assertOpOnChannel(opCh, ClientOperation.CACHE_REPLACE_IF_EQUALS);

        cache.remove(key);

        assertOpOnChannel(opCh, ClientOperation.CACHE_REMOVE_KEY);

        cache.remove(key, value);

        assertOpOnChannel(opCh, ClientOperation.CACHE_REMOVE_IF_EQUALS);

        cache.getAndPut(key, value);

        assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_PUT);

        cache.getAndRemove(key);

        assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_REMOVE);

        cache.getAndReplace(key, value);

        assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_REPLACE);

        cache.putIfAbsent(key, value);

        assertOpOnChannel(opCh, ClientOperation.CACHE_PUT_IF_ABSENT);
    }
}

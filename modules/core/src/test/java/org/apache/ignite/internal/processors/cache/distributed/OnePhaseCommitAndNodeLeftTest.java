/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/**
 * Node left a topology when a one phase transaction committing.
 */
public class OnePhaseCommitAndNodeLeftTest extends GridCommonAbstractTest {

    /** Chache backup count. */
    private int backups = 0;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setBackups(backups)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testZiroBackups() throws Exception {
        startTransactionAndFailPrimary();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOneBackups() throws Exception {
        backups = 1;

        startTransactionAndFailPrimary();
    }

    /**
     * @throws Exception If failed.
     */
    private void startTransactionAndFailPrimary() throws Exception {
        Ignite ignite0 = startGrids(2);

        awaitPartitionMapExchange();

        IgniteCache cache = ignite0.cache(DEFAULT_CACHE_NAME);

        Integer key = primaryKey(ignite(1).cache(DEFAULT_CACHE_NAME));

        ClusterNode node1 = ignite0.affinity(DEFAULT_CACHE_NAME).mapKeyToNode(key);

        assertFalse("Found key is local: " + key + " on node " + node1, node1.isLocal());

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ignite0);

        spi.blockMessages((node, msg) -> {
            if (msg instanceof GridNearTxPrepareRequest) {
                GridNearTxPrepareRequest putReq = (GridNearTxPrepareRequest)msg;

                if (!F.isEmpty(putReq.writes())) {
                    int cacheId = putReq.writes().iterator().next().cacheId();

                    if (cacheId == CU.cacheId(DEFAULT_CACHE_NAME)) {
                        assertTrue(putReq.onePhaseCommit());

                        String nodeName = node.attribute(ATTR_IGNITE_INSTANCE_NAME);

                        return true;
                    }
                }
            }

            return false;
        });

        String testVal = "Tets value";

        IgniteInternalFuture putFut = GridTestUtils.runAsync(() -> {
            cache.put(key, testVal);
        });

        spi.waitForBlocked();

        assertFalse(putFut.isDone());

        ignite(1).close();

        spi.stopBlock();

        putFut.get();

        assertEquals(testVal, cache.get(key));
    }
}

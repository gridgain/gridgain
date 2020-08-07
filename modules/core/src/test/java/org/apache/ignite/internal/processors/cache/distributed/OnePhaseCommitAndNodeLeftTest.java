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
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/**
 * Node left a topology when a one phase transaction committing.
 */
public class OnePhaseCommitAndNodeLeftTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {

        Ignite ignite0 = startGrids(2);

        awaitPartitionMapExchange();

        IgniteCache cache = ignite0.cache(DEFAULT_CACHE_NAME);

        Integer key = primaryKey(ignite(1).cache(DEFAULT_CACHE_NAME));

        ClusterNode node1 = ignite0.affinity(DEFAULT_CACHE_NAME).mapKeyToNode(key);

        assertFalse("Found key is local: " + key + " on node " + node1, node1.isLocal());

        TestRecordingCommunicationSpi.spi(ignite0).blockMessages((node, msg) -> {
            if (msg instanceof GridNearTxPrepareRequest) {
                GridNearTxPrepareRequest putReq = (GridNearTxPrepareRequest)msg;

                if (!F.isEmpty(putReq.writes())) {
                    int cacheId = putReq.writes().iterator().next().cacheId();

                    if (cacheId == CU.cacheId(DEFAULT_CACHE_NAME)) {
                        assertTrue(putReq.onePhaseCommit());

                        String nodeName = node.attribute(ATTR_IGNITE_INSTANCE_NAME);

                        info("Writing a single key to remote node in one pahse transaction: [cahce=" +
                            DEFAULT_CACHE_NAME +
                            ", key=" + putReq.writes().iterator().next().key() +
                            ", node=" + nodeName +
                            ']');

                        ignite(getTestIgniteInstanceIndex(nodeName)).close();
                    }
                }
            }

            return false;
        });

        String testVal = "Tets value";

        cache.put(key, testVal);

        assertEquals(testVal, cache.get(key));
    }
}

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

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/** */
public class ReplicatedSqlRebalanceTest extends AbstractIndexingCommonTest {
    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.REPLICATED)
            // t0d0 check!
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setIndexedTypes(Integer.class, Integer.class);

        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setClientMode(client)
            .setCacheConfiguration(ccfg);
    }

    /** */
    @Test
    public void testWithRebalance() throws Exception {
        IgniteEx ign0 = startGrid(0);

        client = true;

        IgniteEx cli = startGrid(42);

        cli.cache(DEFAULT_CACHE_NAME).put(1, 2);

        int grpId = cli.cachex(DEFAULT_CACHE_NAME).context().groupId();

        TestRecordingCommunicationSpi comm0 = (TestRecordingCommunicationSpi)ign0.configuration().getCommunicationSpi();
        comm0.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                String dstName = node.attribute(ATTR_IGNITE_INSTANCE_NAME);

                if (getTestIgniteInstanceName(1).equals(dstName) && msg instanceof GridDhtPartitionSupplyMessage) {
                    GridDhtPartitionSupplyMessage msg0 = (GridDhtPartitionSupplyMessage)msg;
                    return msg0.groupId() == grpId;
                }

                return false;
            }
        });

        client = false;

        startGrid(1);

        for (Ignite ign : G.allGrids()) {
            List<List<?>> res = ign.cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("select * from Integer")).getAll();

            assertEquals(1, res.size());
            assertEqualsCollections(Arrays.asList(1, 2), res.get(0));
        }

        comm0.stopBlock();
    }
}

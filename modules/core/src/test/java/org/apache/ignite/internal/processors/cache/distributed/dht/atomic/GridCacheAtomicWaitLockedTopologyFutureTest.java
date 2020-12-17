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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheUtilityKey;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.binary.BinaryMetadataKey;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/** */
public class GridCacheAtomicWaitLockedTopologyFutureTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(1000000);
        cfg.setClientFailureDetectionTimeout(1000000);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setAtomicityMode(ATOMIC).
            setWriteSynchronizationMode(FULL_SYNC).
            setBackups(1));

        return cfg;
    }

    /** */
    @Test
    public void testWaitLockedTopologyFuture() throws Exception {
        try {
            IgniteEx crd = startGrids(2);
            awaitPartitionMapExchange();

            TestRecordingCommunicationSpi.spi(crd).record(GridNearAtomicSingleUpdateRequest.class);

            TestRecordingCommunicationSpi.spi(crd).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                    return msg instanceof GridNearAtomicSingleUpdateRequest;
                }
            });

            IgniteInternalFuture<Void> fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    TestRecordingCommunicationSpi.spi(crd).waitForBlocked();

                    GridTestUtils.runAsync(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            IgniteEx client = startClientGrid("client");

                            return null;
                        }
                    });

                    doSleep(1000);

                    GridNearAtomicSingleUpdateRequest r = (GridNearAtomicSingleUpdateRequest) TestRecordingCommunicationSpi.spi(crd).recordedMessages(false).get(0);

                    Field field = U.findField(GridNearAtomicSingleUpdateRequest.class, "topVer");
                    field.set(r, new AffinityTopologyVersion(3, 0));

                    TestRecordingCommunicationSpi.spi(crd).stopBlock();

                    return null;
                }
            });

            IgniteInternalCache<GridCacheUtilityKey, Object> sysCache = crd.utilityCache();

            try(Transaction tx = sysCache.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                sysCache.put(new BinaryMetadataKey(), 0); // Lock topology.

                int[] parts = sysCache.affinity().primaryPartitions(grid(1).localNode());

                crd.cache(DEFAULT_CACHE_NAME).put(parts[0], 0);
            }

            fut.get();
        }
        catch (Throwable r) {
            fail(X.getFullStackTrace(r));
        }
        finally {
            stopAllGrids();
        }
    }
}

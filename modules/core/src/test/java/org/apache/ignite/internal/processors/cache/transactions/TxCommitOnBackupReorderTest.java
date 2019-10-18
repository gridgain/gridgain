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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 */
public class TxCommitOnBackupReorderTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setFailureDetectionTimeout(1000000000L);
        cfg.setClientFailureDetectionTimeout(1000000000L);

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME).
                setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
                setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC).
                setBackups(2));

        return cfg;
    }

    @Test
    public void testBackupCommitReorder() throws Exception {
        try {
            IgniteEx crd = startGrids(3);
            awaitPartitionMapExchange();

            IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

            List<Integer> primaryKeys = primaryKeys(cache, 100);

            CountDownLatch ord = new CountDownLatch(1);

            List<Ignite> backups = backupNodes(primaryKeys.get(0), DEFAULT_CACHE_NAME);

            TestRecordingCommunicationSpi.spi(crd).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return msg instanceof GridDhtTxFinishRequest && node.id().equals(backups.get(0).cluster().localNode().id());
                }
            });

            AtomicReference<GridCacheVersion> tx1Ver = new AtomicReference<>();
            AtomicReference<GridCacheVersion> tx2Ver = new AtomicReference<>();

            IgniteInternalFuture fut0 = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try (Transaction tx = crd.transactions().txStart()) {
                        tx1Ver.set(((TransactionProxyImpl)tx).tx().nearXidVersion());

                        crd.cache(DEFAULT_CACHE_NAME).put(primaryKeys.get(0), 0);

                        ord.countDown();

                        tx.commit();
                    }
                }
            });

            IgniteInternalFuture fut1 = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try (Transaction tx = crd.transactions().txStart()) {
                        tx2Ver.set(((TransactionProxyImpl)tx).tx().nearXidVersion());

                        U.awaitQuiet(ord);

                        crd.cache(DEFAULT_CACHE_NAME).put(primaryKeys.get(0), 0);
                        crd.cache(DEFAULT_CACHE_NAME).put(primaryKeys.get(1), 1);

                        tx.commit();
                    }
                }
            });

            TestRecordingCommunicationSpi.spi(crd).waitForBlocked(2);

            TestRecordingCommunicationSpi.spi(crd).stopBlock(true, new IgnitePredicate<T2<ClusterNode, GridIoMessage>>() {
                @Override public boolean apply(T2<ClusterNode, GridIoMessage> objects) {
                    GridDhtTxFinishRequest req = (GridDhtTxFinishRequest)objects.get2().message();

                    if(req.version().equals(tx2Ver.get()))
                        return true;

                    return false;
                }
            });

            doSleep(1000);

//            @Nullable GridDhtLocalPartition part = ((IgniteEx)backups.get(0)).cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(primaryKeys.get(0));
//
//            assertNotNull(part);
//
//            part.rent(false).get();

            TestRecordingCommunicationSpi.spi(crd).stopBlock();

            fut0.get();
            fut1.get();
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return super.getTestTimeout() * 1000000L;
    }
}

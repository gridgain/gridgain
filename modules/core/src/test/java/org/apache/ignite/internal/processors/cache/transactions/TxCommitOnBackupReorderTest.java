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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
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

import javax.cache.Cache;

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

    @Test
    public void testComplexLocking2() throws Exception {
        try {
            IgniteEx crd = startGrid(0);

            IgniteCache<Integer, Integer> cache = crd.cache(DEFAULT_CACHE_NAME);

            try (Transaction tx = crd.transactions().txStart()) {

                Map m = new LinkedHashMap();
                m.put(0, 0);
                m.put(1, 1);
                m.put(2, 2);

                crd.cache(DEFAULT_CACHE_NAME).putAll(m);

                tx.commit();

                log.info("TX1 Committed");
            }
        }
        finally {
            stopAllGrids();
        }
    }

    @Test
    public void testComplexLocking() throws Exception {
        try {
            IgniteEx crd = startGrid(0);

            IgniteCache<Integer, Integer> cache = crd.cache(DEFAULT_CACHE_NAME);

            List<Integer> primaryKeys = primaryKeys(cache, 10_000);

            CountDownLatch locked = new CountDownLatch(1);
            CountDownLatch tx3Commit = new CountDownLatch(1);
            CountDownLatch tx1Commit = new CountDownLatch(1);
            CyclicBarrier b = new CyclicBarrier(2);

            IgniteInternalFuture fut0 = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try (Transaction tx = crd.transactions().txStart()) {
                        crd.cache(DEFAULT_CACHE_NAME).put(primaryKeys.get(0), -1);

                        U.awaitQuiet(b);

                        locked.countDown();

                        U.awaitQuiet(tx1Commit);

                        tx.commit();

                        log.info("TX1 Committed");
                    }
                }
            });

            IgniteInternalFuture fut1 = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try (Transaction tx = crd.transactions().txStart()) {
                        U.awaitQuiet(locked);

                        Map m = new LinkedHashMap();
                        for (Integer primaryKey : primaryKeys)
                            m.put(primaryKey, 0);

                        crd.cache(DEFAULT_CACHE_NAME).putAll(m);

                        tx.commit();

                        log.info("TX2 Committed");
                    }
                }
            });

            IgniteInternalFuture fut2 = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try (Transaction tx = crd.transactions().txStart()) {
                        crd.cache(DEFAULT_CACHE_NAME).put(primaryKeys.get(primaryKeys.size() - 1), 1);

                        U.awaitQuiet(b);

                        U.awaitQuiet(tx3Commit);

                        tx.commit();

                        log.info("TX3 Committed");
                    }
                }
            });

            U.awaitQuiet(locked);

            doSleep(1000);

            tx1Commit.countDown(); // Trigger chain.

            doSleep(1000);

            tx3Commit.countDown();

            fut0.get();
            fut1.get();
            fut2.get();

            Iterator<Cache.Entry<Integer, Integer>> it = cache.iterator();

            int cnt = 0;
            while (it.hasNext()) {
                Cache.Entry<Integer, Integer> next = it.next();

                assertEquals(0, next.getValue().intValue());

                cnt++;
            }

            assertEquals(primaryKeys.size(), cnt);
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

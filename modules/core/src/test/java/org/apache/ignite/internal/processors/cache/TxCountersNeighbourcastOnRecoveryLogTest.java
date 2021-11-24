/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.processors.cache;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/**
 * Checks logging of partition counters neighbourcast messages.
 */
public class TxCountersNeighbourcastOnRecoveryLogTest extends GridCommonAbstractTest {
    /** */
    private ListeningTestLogger logger = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)
                )
            )
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setGridLogger(logger);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** */
    @Test
    public void test() throws Exception {
        final int NODES_CNT = 3;

        Pattern cntrsDeliveryPattern = Pattern.compile("Starting delivery partition countres to remote nodes.*?msgs=" +
            ".*PartitionUpdateCountersMessage\\{cacheId=1544803905, size=1, cntrs=\\[part=.*?, initCntr=1, cntr=1\\]\\}");

        LogListener cntrsDeliveryLsnr = LogListener.matches(cntrsDeliveryPattern).build();
        LogListener ackLsnr = LogListener.matches("Remote peer acked partition counters delivery [txId=").build();

        logger.registerListener(cntrsDeliveryLsnr);
        logger.registerListener(ackLsnr);

        IgniteEx ignite = startGrids(NODES_CNT);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteEx client = startClientGrid("client");

        IgniteCache<Integer, Integer> cache = client.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(NODES_CNT - 1)
        );

        List<Integer> primaryKeys = primaryKeys(ignite.cache(DEFAULT_CACHE_NAME), 10, 0);

        for (Integer k : primaryKeys)
            cache.put(k, k);

        spi(ignite).blockMessages((node, msg) -> msg instanceof GridDhtTxFinishRequest);
        spi(client).blockMessages((node, msg) -> msg instanceof GridCacheTxRecoveryResponse);
        spi(grid(1)).blockMessages((node, msg) -> {
            if (msg instanceof GridCacheTxRecoveryResponse) {
                GridCacheTxRecoveryResponse m = (GridCacheTxRecoveryResponse) msg;

                if (!m.success())
                    return false;

                GridCacheTxRecoveryResponse resp =
                    new GridCacheTxRecoveryResponse(m.version(), m.futureId(), m.miniId(), false, m.addDepInfo);

                try {
                    grid(1).context().io().sendToGridTopic(node.id(), GridTopic.TOPIC_CACHE, resp, SYSTEM_POOL);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }

                return true;
            }
            else
                return false;
        });

        IgniteInternalFuture fut = runMultiThreadedAsync(() -> {
            try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
                int key = primaryKeys.get(ThreadLocalRandom.current().nextInt(10));

                Integer v = cache.get(key);

                cache.put(key, v == null ? 0 : v);

                tx.commit();
            }
        }, 1, "testTx");

        doSleep(500);

        stopGrid(0);
        stopGrid("client");

        awaitPartitionMapExchange();

        fut.get();

        assertTrue(cntrsDeliveryLsnr.check());
        assertTrue(ackLsnr.check());
    }
}

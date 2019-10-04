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

package org.apache.ignite.internal.processors.cache;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for client nodes with slow discovery.
 */
public class ClientSlowDiscoveryTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Cache configuration. */
    private final CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setReadFromBackup(false)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, 64));

    /** Client mode. */
    private boolean clientMode;

    /** Communication SPI supplier. */
    private Supplier<CommunicationSpi> communicationSpiSupplier = TestRecordingCommunicationSpi::new;

    /** Discovery SPI supplier. */
    private Supplier<DiscoverySpi> discoverySpiSupplier = CustomMessageInterceptingDiscoverySpi::new;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);
        cfg.setCacheConfiguration(ccfg);
        cfg.setCommunicationSpi(communicationSpiSupplier.get());
        cfg.setDiscoverySpi(discoverySpiSupplier.get());
        cfg.setClientMode(clientMode);

        return cfg;
    }

    /**
     *
     */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test check that client join works well if cache configured on it stopped on server nodes
     * but discovery event about cache stop is not delivered to client node immediately.
     * When client node joins to cluster it sends SingleMessage to coordinator.
     * During this time topology on server nodes can be changed,
     * because client exchange doesn't require acknowledgement for SingleMessage on coordinator.
     * Delay is simulated by blocking sending this SingleMessage and resume sending after topology is changed.
     */
    @Test
    public void testClientJoinAndCacheStop() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3);

        awaitPartitionMapExchange();

        for (int k = 0; k < 64; k++)
            crd.cache(CACHE_NAME).put(k, k);

        clientMode = true;

        TestRecordingCommunicationSpi clientCommSpi = new TestRecordingCommunicationSpi();

        // Delay client join process.
        clientCommSpi.blockMessages((node, msg) -> {
            if (!(msg instanceof GridDhtPartitionsSingleMessage))
                return false;

            GridDhtPartitionsSingleMessage singleMsg = (GridDhtPartitionsSingleMessage) msg;

            return Optional.ofNullable(singleMsg.exchangeId())
                    .map(GridDhtPartitionExchangeId::topologyVersion)
                    .filter(topVer -> topVer.equals(new AffinityTopologyVersion(4, 0)))
                    .isPresent();
        });

        communicationSpiSupplier = () -> clientCommSpi;

        CustomMessageInterceptingDiscoverySpi clientDiscoSpi = new CustomMessageInterceptingDiscoverySpi();

        CountDownLatch clientDiscoSpiBlock = new CountDownLatch(1);

        // Delay cache destroying on client node.
        clientDiscoSpi.interceptor = (msg) -> {
            if (!(msg instanceof DynamicCacheChangeBatch))
                return;

            DynamicCacheChangeBatch cacheChangeBatch = (DynamicCacheChangeBatch) msg;

            boolean hasCacheStopReq = cacheChangeBatch.requests().stream()
                    .anyMatch(req -> req.stop() && req.cacheName().equals(CACHE_NAME));

            if (hasCacheStopReq)
                U.awaitQuiet(clientDiscoSpiBlock);
        };

        discoverySpiSupplier = () -> clientDiscoSpi;

        IgniteInternalFuture<IgniteEx> clientStartFut = GridTestUtils.runAsync(() -> startGrid(3));

        // Wait till client node starts join process.
        clientCommSpi.waitForBlocked();

        // Destroy cache on server nodes.
        crd.destroyCache(CACHE_NAME);

        // Resume client join.
        clientCommSpi.stopBlock();

        // Client join should succeed.
        IgniteEx client = clientStartFut.get();

        IgniteCache<Object, Object> clientCache = client.cache(CACHE_NAME);

        Assert.assertNotNull("Cache should exists on client node", clientCache);

        IgniteInternalFuture<?> cacheGet = GridTestUtils.runAsync(() -> clientCache.get(0));

        try {
            cacheGet.get(5_000); // Reasonable timeout.

            fail("Cache get operation should throw " + CacheStoppedException.class);
        }
        catch (Exception e) {
            assertTrue("Got unexpected exception during cache get " + e,
                    X.hasCause(e, CacheStoppedException.class));
        }
        finally {
            // Resume processing cache destroy on client node.
            clientDiscoSpiBlock.countDown();
        }

        // Wait till cache destroyed on client node.
        GridTestUtils.waitForCondition(() -> {
            AffinityTopologyVersion topVer = client.context().cache().context().exchange().lastFinishedFuture()
                    .topologyVersion();

            // Cache destroy version.
            return topVer.equals(new AffinityTopologyVersion(4, 1));
        }, 5_000); // Reasonable timeout.

        Assert.assertNull("Cache should be destroyed on client node", client.cache(CACHE_NAME));
    }

    /**
     * Test check that transaction remap works correcrtly if transaction is mapped on some topology version,
     * topology changed on server nodes, but discovery event about another cache stop
     * is not delivered to client node immediately.
     */
    @Test
    public void testTransactionRemap() throws Exception {
        startGrid(0);

        clientMode = true;

        NodeJoinInterceptingDiscoverySpi clientDiscoSpi = new NodeJoinInterceptingDiscoverySpi();

        CountDownLatch clientDiscoSpiBlock = new CountDownLatch(1);

        // Delay node join of second client.
        clientDiscoSpi.interceptor = msg -> {
            if (msg.nodeId().toString().endsWith("2"))
                U.awaitQuiet(clientDiscoSpiBlock);
        };

        discoverySpiSupplier = () -> clientDiscoSpi;

        IgniteEx clnt = startGrid(1);

        clnt.cache(CACHE_NAME).put(1, 100);

        discoverySpiSupplier = TcpDiscoverySpi::new;

        startGrid(2);

        IgniteInternalFuture<?> txFut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = clnt.transactions().txStart(
                TransactionConcurrency.PESSIMISTIC,
                TransactionIsolation.REPEATABLE_READ)
            ) {
                // This operation remaps transaction to new version.
                clnt.cache(CACHE_NAME).put(1, 1);

                // Need to use the same key to avoid enlisting new entry with topology version await.
                clnt.cache(CACHE_NAME).remove(1);

                tx.commit();
            }
        });

        try {
            txFut.get(1, TimeUnit.SECONDS);
        }
        catch (IgniteFutureTimeoutCheckedException te) {
            // Expected.
        }
        finally {
            clientDiscoSpiBlock.countDown();
        }

        // After resume second client join, transaction should succesfully await new affinity and commit.
        txFut.get();

        // Check transaction commit.
        Assert.assertNull("Transaction is not committed.", clnt.cache(CACHE_NAME).get(1));
    }

    /**
     * Test check transaction timeout if client waits for a topology version, but discovery event about topology change
     * is not delivered to client node immediately.
     */
    @Test
    public void testTransactionRemapWithTimeout() throws Exception {
        startGrid(0);

        clientMode = true;

        NodeJoinInterceptingDiscoverySpi clientDiscoSpi = new NodeJoinInterceptingDiscoverySpi();

        CountDownLatch clientDiscoSpiBlock = new CountDownLatch(1);

        // Delay node join of second client.
        clientDiscoSpi.interceptor = msg -> {
            if (msg.nodeId().toString().endsWith("2"))
                U.awaitQuiet(clientDiscoSpiBlock);
        };

        discoverySpiSupplier = () -> clientDiscoSpi;

        IgniteEx clnt = startGrid(1);

        clnt.cache(CACHE_NAME).put(1, 100);

        discoverySpiSupplier = TcpDiscoverySpi::new;

        startGrid(2);

        IgniteInternalFuture<?> txFut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = clnt.transactions().txStart(
                TransactionConcurrency.PESSIMISTIC,
                TransactionIsolation.REPEATABLE_READ,
                2_000, // 2 seconds.
                1_000_000)
            ) {
                // This operation remaps transaction to new version.
                clnt.cache(CACHE_NAME).put(1, 1);

                // Need to use the same key to avoid enlisting new entry with topology version await.
                clnt.cache(CACHE_NAME).remove(1);

                tx.commit();
            }
        });

        try {
            txFut.get(4, TimeUnit.SECONDS);
        }
        catch (IgniteFutureTimeoutCheckedException te) {
            // Expected.
        }
        finally {
            clientDiscoSpiBlock.countDown();
        }

        // After resume second client join, transaction should be timed out.
        GridTestUtils.assertThrowsWithCause((Callable<Object>) txFut::get, TransactionTimeoutException.class);

        // Check that transaction is not commited.
        Assert.assertNotNull("Transaction committed.", clnt.cache(CACHE_NAME).get(1));
    }

    /**
     *
     */
    static class NodeJoinInterceptingDiscoverySpi extends TcpDiscoverySpi {
        /** Interceptor. */
        private volatile IgniteInClosure<TcpDiscoveryNodeAddFinishedMessage> interceptor;

        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryNodeAddFinishedMessage && interceptor != null)
                interceptor.apply((TcpDiscoveryNodeAddFinishedMessage) msg);
        }
    }

    /**
     *
     */
    static class CustomMessageInterceptingDiscoverySpi extends TcpDiscoverySpi {
        /** Interceptor. */
        private volatile IgniteInClosure<DiscoveryCustomMessage> interceptor;

        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (!(msg instanceof TcpDiscoveryCustomEventMessage))
                return;

            TcpDiscoveryCustomEventMessage cm = (TcpDiscoveryCustomEventMessage)msg;

            DiscoveryCustomMessage delegate;

            try {
                DiscoverySpiCustomMessage custMsg = cm.message(marshaller(),
                        U.resolveClassLoader(ignite().configuration()));

                assertNotNull(custMsg);

                delegate = ((CustomMessageWrapper)custMsg).delegate();
            }
            catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }

            if (interceptor != null)
                interceptor.apply(delegate);
        }
    }
}

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
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
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
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for
 */
public class ClientDelayedJoinTest extends GridCommonAbstractTest {
    private static final String CACHE_NAME = "cache";
    private final CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME)
            .setReadFromBackup(false)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, 64));
    private boolean clientMode;
    private Supplier<CommunicationSpi> communicationSpiSupplier = TestRecordingCommunicationSpi::new;
    private Supplier<DiscoverySpi> discoverySpiSupplier = CustomMessageInterceptingDiscoverySpi::new;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(ccfg);
        cfg.setConsistentId(igniteInstanceName);
        cfg.setCommunicationSpi(communicationSpiSupplier.get());
        cfg.setDiscoverySpi(discoverySpiSupplier.get());
        cfg.setClientMode(clientMode);

        return cfg;
    }

    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

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

    static class CustomMessageInterceptingDiscoverySpi extends TcpDiscoverySpi {
        private volatile IgniteInClosure<DiscoveryCustomMessage> interceptor;

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

        static CustomMessageInterceptingDiscoverySpi spi(Ignite node) {
            return (CustomMessageInterceptingDiscoverySpi) node.configuration().getDiscoverySpi();
        }
    }
}

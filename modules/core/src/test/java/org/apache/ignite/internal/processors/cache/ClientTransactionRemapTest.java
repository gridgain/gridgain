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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test to check that client remap in transaction works correctly.
 */
public class ClientTransactionRemapTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Discovery SPI supplier. */
    private Supplier<DiscoverySpi> discoverySpiSupplier = TcpDiscoverySpi::new;

    /** Client mode. */
    private boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(
            new CacheConfiguration(CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        );

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
     *
     */
    @Test
    public void testRemapWithDelayedDiscoveryEvent() throws Exception {
        IgniteEx crd = startGrid(0);

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
}

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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool;
import org.apache.ignite.spi.communication.tcp.internal.IncomingConnectionHandler;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage2;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.TestDependencyResolver;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Matchers.any;

/**
 * GridDhtCacheEntry::toString leads to system "deadlock" on the timeoutWorker.
 */
public class TxDeadlockOnEntryToStringTest extends GridCommonAbstractTest {
    /** Test key. */
    private static final int TEST_KEY = 1;

    /**
     * Mark that incoming connect must be rejected.
     */
    private static final AtomicBoolean rejectHandshake = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        return cfg;
    }

    /**
     * We removed locks from toString on Entry. The case, a thread X lock entry, after that trying to do a handshake
     * with connecting node. But, handshake fails on the first attempt. Between these events, timeout worked trying to
     * print this entry, but I locked. The thread X can't reconnect, because timeout worker hangs.
     */
    @Test
    public void testDeadlockOnTimeoutWorkerAndToString() throws Exception {
        // Setup
        TestDependencyResolver nearDepRslvr = new TestDependencyResolver();
        IgniteEx nearNode = startGrid(0, nearDepRslvr);

        TestDependencyResolver incomingDepRslvr = new TestDependencyResolver(this::resolve);
        IgniteEx incomingNode = startGrid(1, incomingDepRslvr);

        GridTimeoutProcessor tp = nearNode.context().timeout();
        ConnectionClientPool pool = nearDepRslvr.getDependency(ConnectionClientPool.class);

        GridCacheEntryEx ex = getEntry(nearNode, DEFAULT_CACHE_NAME, TEST_KEY);

        // Act
        ex.lockEntry(); // Lock entry in current thread

        // Print the entry from another thread via timeObject.
        CountDownLatch entryPrinted = new CountDownLatch(1);
        CountDownLatch entryReadyToPrint = new CountDownLatch(1);
        tp.addTimeoutObject(new EntryPrinterTimeoutObject(ex, entryPrinted, entryReadyToPrint));

        entryReadyToPrint.await();

        // Try to do first handshake with hangs, after reconnect handshake should be passed.
        pool.forceClose();

        rejectHandshake.set(true);

        nearNode.configuration().getCommunicationSpi().sendMessage(incomingNode.localNode(), UUIDCollectionMessage.of(UUID.randomUUID()));

        // Check
        assertTrue(GridTestUtils.waitForCondition(() -> entryPrinted.getCount() == 0, 5_000));
    }

    /**
     * @param node Node.
     * @param cacheName Cache name.
     * @param key Key.
     */
    private GridCacheEntryEx getEntry(IgniteEx node, String cacheName, Object key) {
        return node.cachex(cacheName).context().cache()
            .entryEx(
                node.cachex(cacheName).context().toCacheKeyObject(key),
                node.context().discovery().topologyVersionEx()
            );
    }

    /**
     * Call toString via time interval.
     */
    private class EntryPrinterTimeoutObject implements GridTimeoutObject {
        /** Id. */
        private final IgniteUuid id = IgniteUuid.randomUuid();
        /** Entry. */
        private final GridCacheEntryEx entry;
        /** Entry printed. */
        private final CountDownLatch entryPrinted;
        /** Entry ready to print. */
        private final CountDownLatch entryReadyToPrint;

        /**
         * @param entry Entry.
         * @param entryPrinted Entry printed.
         * @param entryReadyToPrint Entry ready to print.
         */
        private EntryPrinterTimeoutObject(GridCacheEntryEx entry, CountDownLatch entryPrinted,
            CountDownLatch entryReadyToPrint) {
            this.entry = entry;
            this.entryPrinted = entryPrinted;
            this.entryReadyToPrint = entryReadyToPrint;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid timeoutId() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            entryReadyToPrint.countDown();

            entry.toString();

            entryPrinted.countDown();
        }
    }

    /**
     * The method reference implementation of {@link DependencyResolver}. It adds an additional behavior to {@link
     * IncomingConnectionHandler}.
     *
     * @param instance Delegated instance.
     */
    private <T> T resolve(T instance) {
        if (instance instanceof IncomingConnectionHandler) {
            IncomingConnectionHandler hnd = Mockito.spy((IncomingConnectionHandler)instance);

            Mockito.doAnswer(inv -> {
                GridNioSession ses = (GridNioSession)inv.getArguments()[0];
                Message msg = (Message)inv.getArguments()[1];

                if (rejectHandshake.get() && msg instanceof HandshakeMessage2) {
                    rejectHandshake.set(false);

                    ses.close();

                    return null;
                }

                hnd.onMessage(ses, msg);

                return null;
            }).when(hnd).onMessageSent(any(), any());

            return (T)hnd;
        }

        return instance;
    }
}

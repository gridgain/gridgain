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
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.resource.WrappableResource;
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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * GridDhtCacheEntry::toString leads to system "deadlock" on the timeoutWorker.
 */
public class TxDeadlockOnEntryToStringTest extends GridCommonAbstractTest {
    /** Test key. */
    private static final int TEST_KEY = 1;

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
        IgniteEx nearNode = startGrid(0);
        IgniteEx incomingNode = startGrid(1, new BlockingIncomingHandler());

        GridTimeoutProcessor tp = nearNode.context().timeout();
        ConnectionClientPool pool = getInstance(nearNode, ConnectionClientPool.class);

        BlockingIncomingHandler incomingHandler = (BlockingIncomingHandler)getInstance(incomingNode, IncomingConnectionHandler.class);

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

        incomingHandler.rejectHandshake();

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
     * Handler makes reject on handshake and delegate logic of work.
     */
    private class BlockingIncomingHandler extends IncomingConnectionHandler implements WrappableResource<IncomingConnectionHandler> {
        /**
         * Mark that incoming connect must be rejected.
         */
        private final AtomicBoolean rejectHandshake = new AtomicBoolean(false);
        /**
         * Delegate of original class.
         */
        private volatile IncomingConnectionHandler delegate;

        /** {@inheritDoc} */
        @Override public void onSessionWriteTimeout(GridNioSession ses) {
            delegate.onSessionWriteTimeout(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionIdleTimeout(GridNioSession ses) {
            delegate.onSessionIdleTimeout(ses);
        }

        /** {@inheritDoc} */
        @Override public void onMessageSent(GridNioSession ses, Message msg) {
            delegate.onMessageSent(ses, msg);
        }

        /** {@inheritDoc} */
        @Override public void onFailure(FailureType failureType, Throwable failure) {
            delegate.onFailure(failureType, failure);
        }

        /** {@inheritDoc} */
        @Override public void onConnected(GridNioSession ses) {
            delegate.onConnected(ses);
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
            delegate.onDisconnected(ses, e);
        }

        /** {@inheritDoc} */
        @Override public void onMessage(GridNioSession ses, Message msg) {
            if (rejectHandshake.get() && msg instanceof HandshakeMessage2) {
                rejectHandshake.set(false);

                ses.close();

                return;
            }

            delegate.onMessage(ses, msg);
        }

        /** {@inheritDoc} */
        @Override public IncomingConnectionHandler wrap(IncomingConnectionHandler rsrc) {
            delegate = rsrc;

            return this;
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            delegate.stop();
        }

        /**
         * Mark that handshake must be rejected.
         */
        void rejectHandshake() {
            rejectHandshake.set(true);
        }
    }
}

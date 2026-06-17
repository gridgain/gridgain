/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.client.thin;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.client.thin.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests that {@link ClientCache#sizeLong(org.apache.ignite.cache.CachePeekMode...)} returns the full {@code long}
 * cache size, while the deprecated {@link ClientCache#size(org.apache.ignite.cache.CachePeekMode...)} fails fast
 * instead of silently overflowing when the size exceeds {@link Integer#MAX_VALUE}.
 * <p>
 * The server-reported size is stubbed at the channel level so the large-size scenario can be exercised without
 * actually populating a cache with more than {@link Integer#MAX_VALUE} entries.
 */
public class ClientCacheSizeLongTest {
    /** Size that does not fit into the {@code int} type. */
    private static final long OVERFLOW_SIZE = Integer.MAX_VALUE + 1L;

    /** */
    @Test
    public void testSizeReturnsValueWithinIntRange() {
        assertEquals(42, cacheReportingSize(42).size());
        assertEquals(Integer.MAX_VALUE, cacheReportingSize(Integer.MAX_VALUE).size());
    }

    /** */
    @Test
    public void testSizeLongReturnsValueWithinIntRange() {
        assertEquals(42L, cacheReportingSize(42).sizeLong());
        assertEquals(Integer.MAX_VALUE, cacheReportingSize(Integer.MAX_VALUE).sizeLong());
    }

    /** */
    @Test
    public void testSizeLongReturnsValueExceedingIntRange() {
        assertEquals(OVERFLOW_SIZE, cacheReportingSize(OVERFLOW_SIZE).sizeLong());
    }

    /** */
    @Test
    public void testSizeFailsWhenValueExceedsIntRange() {
        GridTestUtils.assertThrows(
            null,
            () -> cacheReportingSize(OVERFLOW_SIZE).size(),
            ClientException.class,
            "Cache size exceeded maximum value for int type"
        );
    }

    /**
     * Creates a cache backed by a channel that always reports the given cache size for the
     * {@link ClientOperation#CACHE_GET_SIZE} operation.
     *
     * @param size Cache size to report from the server.
     * @return Cache instance.
     */
    private static TcpClientCache<Integer, Integer> cacheReportingSize(long size) {
        BiFunction<ClientChannelConfiguration, ClientConnectionMultiplexer, ClientChannel> chFactory =
            (cfg, hnd) -> new FixedSizeClientChannel(size);

        ClientConfiguration ccfg = new ClientConfiguration()
            .setAddresses("127.0.0.1:10800")
            .setAffinityAwarenessEnabled(false);

        ReliableChannel ch = new ReliableChannel(chFactory, ccfg, null);

        ch.channelsInit();

        return new TcpClientCache<>("test", ch, null, null, null);
    }

    /**
     * Client channel stub that answers every {@link ClientOperation#CACHE_GET_SIZE} request with a fixed
     * cache size encoded on the wire exactly as the real server does (a single {@code long}).
     */
    private static class FixedSizeClientChannel implements ClientChannel {
        /** Cache size reported to the client. */
        private final long size;

        /** */
        private final UUID serverNodeId = UUID.randomUUID();

        /** */
        private FixedSizeClientChannel(long size) {
            this.size = size;
        }

        /** {@inheritDoc} */
        @Override public <T> T service(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter,
            Function<PayloadInputChannel, T> payloadReader) {
            if (payloadReader == null)
                return null;

            byte[] data;

            try (BinaryHeapOutputStream out = new BinaryHeapOutputStream(Long.BYTES)) {
                out.writeLong(size);

                data = out.arrayCopy();
            }

            ByteBuffer payload = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

            return payloadReader.apply(new PayloadInputChannel(this, payload));
        }

        /** {@inheritDoc} */
        @Override public <T> CompletableFuture<T> serviceAsync(ClientOperation op,
            Consumer<PayloadOutputChannel> payloadWriter, Function<PayloadInputChannel, T> payloadReader) {
            CompletableFuture<T> fut = new CompletableFuture<>();

            try {
                fut.complete(service(op, payloadWriter, payloadReader));
            }
            catch (Throwable t) {
                fut.completeExceptionally(t);
            }

            return fut;
        }

        /** {@inheritDoc} */
        @Override public ProtocolContext protocolCtx() {
            return new ProtocolContext(ProtocolVersion.CURRENT_VER, null);
        }

        /** {@inheritDoc} */
        @Override public UUID serverNodeId() {
            return serverNodeId;
        }

        /** {@inheritDoc} */
        @Override public AffinityTopologyVersion serverTopologyVersion() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void addTopologyChangeListener(Consumer<ClientChannel> lsnr) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void addNotificationListener(ClientNotificationType type, Long rsrcId,
            NotificationListener lsnr) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void removeNotificationListener(ClientNotificationType type, Long rsrcId) {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public boolean closed() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            /* No-op. */
        }
    }
}

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

package org.apache.ignite.internal.client.thin;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.client.ClientDisconnectListener;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryByteBufferInputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.client.thin.ClientNotificationType.CONTINUOUS_QUERY_EVENT;
import static org.apache.ignite.internal.client.thin.TcpClientCache.JAVA_PLATFORM;

/**
 * Handler for {@link ContinuousQuery} listeners and JCache cache entry listeners.
 */
public class ClientCacheEntryListenerHandler<K, V> implements NotificationListener, AutoCloseable {
    /** "Keep binary" flag mask. */
    private static final byte KEEP_BINARY_FLAG_MASK = 0x01;

    /** */
    private final Cache<K, V> jCacheAdapter;

    /** */
    private final ReliableChannel ch;

    /** */
    private final boolean keepBinary;

    /** */
    private final ClientUtils utils;

    private final CompletableFuture<T2<ClientChannel, Long>> clientChFut = new CompletableFuture<>();

    private final IgniteClientFuture<Void> closeFut = new IgniteClientFutureImpl<>(new CompletableFuture<>());

    private final AtomicInteger state = new AtomicInteger(0);

    /** */
    private volatile CacheEntryUpdatedListener<K, V> locLsnr;

    /** */
    private volatile ClientDisconnectListener disconnectLsnr;

    /** */
    ClientCacheEntryListenerHandler(
        Cache<K, V> jCacheAdapter,
        ReliableChannel ch,
        ClientBinaryMarshaller marsh,
        boolean keepBinary
    ) {
        this.jCacheAdapter = jCacheAdapter;
        this.ch = ch;
        this.keepBinary = keepBinary;
        utils = new ClientUtils(marsh);
    }

    /**
     * Send request to the server and start
     */
    public void startListen(
        CacheEntryUpdatedListener<K, V> locLsnr,
        ClientDisconnectListener disconnectLsnr,
        Factory<? extends CacheEntryEventFilter<? super K, ? super V>> rmtFilterFactory,
        int pageSize,
        long timeInterval,
        boolean includeExpired
    ) {
        ClientUtils.syncResult(
                startListenAsync(locLsnr, disconnectLsnr, rmtFilterFactory, pageSize, timeInterval, includeExpired)
        );
   }

    /**
     * Send request to the server and start listening asynchronously.
     */
    public IgniteClientFuture<Void> startListenAsync(
        CacheEntryUpdatedListener<K, V> locLsnr,
        ClientDisconnectListener disconnectLsnr,
        Factory<? extends CacheEntryEventFilter<? super K, ? super V>> rmtFilterFactory,
        int pageSize,
        long timeInterval,
        boolean includeExpired
    ) {
        assert locLsnr != null;
        if (state.compareAndSet(0, 1)) {
                this.locLsnr = locLsnr;
                this.disconnectLsnr = disconnectLsnr;

                Consumer<PayloadOutputChannel> qryWriter = queryWriter(pageSize, timeInterval, includeExpired, rmtFilterFactory);
                Function<PayloadInputChannel, T2<ClientChannel, Long>> qryReader = queryReader();

                ch.serviceAsync(ClientOperation.QUERY_CONTINUOUS, qryWriter, qryReader)
                        .whenComplete((params, err) -> {
                            if (err != null) {
                                Throwable cause = (err instanceof CompletionException) ? err.getCause() : err;
                                Throwable publicErr = (cause instanceof ClientError) ? new ClientException(cause) : cause;
                                clientChFut.completeExceptionally(publicErr);
                            } else {
                                clientChFut.complete(params);
                            }
                        });
        }

        return new IgniteClientFutureImpl<>(clientChFut.thenAccept(params -> {}));
    }

    /** */
    private Consumer<PayloadOutputChannel> queryWriter(
        int pageSize,
        long timeInterval,
        boolean includeExpired,
        Factory<? extends CacheEntryEventFilter<? super K, ? super V>> rmtFilterFactory
    ) {
        return payloadCh -> {
            BinaryOutputStream out = payloadCh.out();

            out.writeInt(ClientUtils.cacheId(jCacheAdapter.getName()));
            out.writeByte(keepBinary ? KEEP_BINARY_FLAG_MASK : 0);
            out.writeInt(pageSize);
            out.writeLong(timeInterval);
            out.writeBoolean(includeExpired);

            if (rmtFilterFactory == null)
                out.writeByte(GridBinaryMarshaller.NULL);
            else {
                utils.writeObject(out, rmtFilterFactory);
                out.writeByte(JAVA_PLATFORM);
            }
        };
    }

    /** */
    private Function<PayloadInputChannel, T2<ClientChannel, Long>> queryReader() {
        return payloadCh -> {
            ClientChannel ch = payloadCh.clientChannel();
            Long rsrcId = payloadCh.in().readLong();

            ch.addNotificationListener(CONTINUOUS_QUERY_EVENT, rsrcId, this);

            return new T2<>(ch, rsrcId);
        };
    }

    /** {@inheritDoc} */
    @Override public void acceptNotification(ByteBuffer payload, Exception err) {
        if (err == null && payload != null) {
            BinaryInputStream in = BinaryByteBufferInputStream.create(payload);

            int cnt = in.readInt();

            List<CacheEntryEvent<? extends K, ? extends V>> evts = new ArrayList<>(cnt);

            for (int i = 0; i < cnt; i++) {
                K key = utils.readObject(in, keepBinary);
                V oldVal = utils.readObject(in, keepBinary);
                V val = utils.readObject(in, keepBinary);
                byte evtTypeByte = in.readByte();

                EventType evtType = eventType(evtTypeByte);

                if (evtType == null)
                    onChannelClosed(new ClientException("Unknown event type: " + evtTypeByte));

                evts.add(new CacheEntryEventImpl<>(jCacheAdapter, evtType, key, oldVal, val));
            }

            locLsnr.onUpdated(evts);
        }
    }

    /** {@inheritDoc} */
    @Override public void onChannelClosed(Exception reason) {
        ClientDisconnectListener lsnr = disconnectLsnr;

        if (lsnr != null)
            lsnr.onDisconnected(reason);

        U.closeQuiet(this);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ClientUtils.syncResult(closeAsync());
    }

    /**
     * Stop listening asynchronously.
     */
    public IgniteClientFuture<Void> closeAsync() {
        // Seems a bit silly but maintains compatibility with the previous version.
        if (state.get() == 0) {
            return IgniteClientFutureImpl.completedFuture(null);
        }

        if (state.compareAndSet(1, 2)) {
            clientChFut.thenCompose(p -> {
                        ClientChannel clientCh = p.get1();
                        Long rsrcId = p.get2();

                        if (clientCh.closed()) {
                            return IgniteClientFutureImpl.completedFuture(null);
                        }

                        clientCh.removeNotificationListener(CONTINUOUS_QUERY_EVENT, rsrcId);

                        return clientCh.serviceAsync(
                                ClientOperation.RESOURCE_CLOSE,
                                ch -> ch.out().writeLong(rsrcId),
                                null
                        );
                    })
                    .whenComplete((v, err) -> {
                        CompletableFuture<Void> cp = closeFut.toCompletableFuture();
                        if (err != null) {
                            cp.completeExceptionally(err);
                        } else {
                            cp.complete(null);
                        }
                    });
        }

        return closeFut;
    }

    static <T> IgniteClientFuture<T> grabReference(AtomicReference<IgniteClientFuture<T>> ref) {
        IgniteClientFuture<T> fut = ref.get();
        if (fut == null) {
            IgniteClientFuture<T> newFut = new IgniteClientFutureImpl<>(new CompletableFuture<>());
            if (ref.compareAndSet(null, newFut)) {
                fut = newFut;
            } else {
                fut = ref.get();
            }
        }

        return fut;
    }

    /**
     * Client channel.
     */
    public ClientChannel clientChannel() {
        return ClientUtils.syncResult(clientChFut.thenApply(T2::get1));
    }

    /** */
    private EventType eventType(byte evtTypeByte) {
        switch (evtTypeByte) {
            case 0: return EventType.CREATED;
            case 1: return EventType.UPDATED;
            case 2: return EventType.REMOVED;
            case 3: return EventType.EXPIRED;
            default: return null;
        }
    }

    /**
     *
     */
    private static class CacheEntryEventImpl<K, V> extends CacheEntryEvent<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Key. */
        private final K key;

        /** Old value. */
        private final V oldVal;

        /** Value. */
        private final V val;

        /**
         *
         */
        private CacheEntryEventImpl(Cache<K, V> src, EventType evtType, K key, V oldVal, V val) {
            super(src, evtType);

            this.key = key;
            this.oldVal = oldVal;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public K getKey() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public V getValue() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public V getOldValue() {
            return oldVal;
        }

        /** {@inheritDoc} */
        @Override public boolean isOldValueAvailable() {
            return oldVal != null;
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> clazz) {
            if (clazz.isAssignableFrom(getClass()))
                return clazz.cast(this);

            throw new IllegalArgumentException("Unwrapping to class is not supported: " + clazz);
        }
    }
}

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

package org.apache.ignite.internal.client.thin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.client.datastreamer.ClientDataStreamer;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.processors.platform.client.ClientPlatform;
import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.stream.StreamReceiver;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteDataStreamer.DFLT_PER_NODE_BUFFER_SIZE;
import static org.apache.ignite.IgniteDataStreamer.DFLT_UNLIMIT_TIMEOUT;
import static org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerFlags.*;

/**
 * Implementation of {@link ClientDataStreamer} over TCP protocol.
 *
 * <p>Each batch is sent as a self-contained {@code DATA_STREAMER_START} request with
 * {@code FLUSH | CLOSE} flags.</p>
 *
 * <p>When partition awareness is enabled, a separate batch stream is maintained per primary cluster
 * node so that each batch is sent directly to the node that owns the keys. When partition awareness
 * is disabled or the affinity mapping is not yet available, all entries share a single stream keyed
 * by {@link #UNKNOWN_NODE}.</p>
 */
class TcpClientDataStreamer<K, V> implements ClientDataStreamer<K, V> {

    /**
     * Sentinel value passed as perNodeBufferSize / perThreadBufferSize to let the server
     * use its own defaults.
     */
    private static final int SERVER_BUFFER_SIZE_AUTO = -1;

    /** Sentinel for disabled timeout (matches {@code IgniteDataStreamer.DFLT_UNLIMIT_TIMEOUT}). */
    private static final long TIMEOUT_DISABLED = -1L;

    /**
     * Map-key sentinel used when the primary node for a key is not yet known (partition awareness
     * disabled or affinity mapping not yet available). {@code ConcurrentHashMap} does not allow
     * {@code null} keys, so we use a fixed all-zero UUID instead.
     */
    private static final UUID UNKNOWN_NODE = new UUID(0L, 0L);

    /** Cache name. */
    private final String cacheName;

    /** Cache ID. */
    private final int cacheId;

    /** Channel. */
    private final ReliableChannel ch;

    /** Serializer/deserializer. */
    private final ClientUtils serDes;

    /** Flag enabling overwriting existing values in cache; {@code false} by default. */
    private volatile boolean allowOverwrite;

    /** Flag disabling write-through to the underlying cache store; {@code false} by default. */
    private volatile boolean skipStore;

    /** Flag indicating that objects should be kept in binary format when passed to the stream receiver. */
    private volatile boolean keepBinary;

    /** Per-node key-value pairs buffer size. */
    private volatile int perNodeBufferSize;

    /** Maximum number of parallel send operations per server node. */
    private volatile int perNodeParallelOperations;

    /** Timeout in milliseconds; {@link #TIMEOUT_DISABLED} means unlimited. */
    private volatile long timeout;

    /** Auto-flush interval in milliseconds; 0 means disabled. */
    private volatile long autoFlushInterval;

    /** Optional custom stream receiver; {@code null} means the default upsert/remove behaviour. */
    private volatile StreamReceiver<K, V> receiver;

    /**
     * Flags byte derived from {@link #allowOverwrite}, {@link #skipStore}, {@link #keepBinary}.
     * Always includes {@code FLUSH | CLOSE} for one-shot semantics.
     */
    private volatile byte flags;

    /**
     * Per-node batch streams. Key: primary node UUID, or {@link #UNKNOWN_NODE} when partition
     * awareness is disabled or the affinity mapping is not yet available for the entry's key.
     */
    private final ConcurrentHashMap<UUID, PartitionContext<K, V>> partitions = new ConcurrentHashMap<>();

    /** Logger. */
    private final IgniteLogger log;

    /** Set to {@code true} once {@link #close(boolean)} has been called. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** Completed when {@link #close(boolean)} finishes. */
    private final CompletableFuture<Void> closeFut = new CompletableFuture<>();

    /** Guards {@link #autoFlushInterval} and {@link #flushFut} updates. */
    private final Lock autoFlushLock = new ReentrantLock();

    /** Single-thread executor for batch sends and auto-flush tasks. */
    private final ScheduledExecutorService scheduler;

    /** Scheduled auto-flush task; {@code null} when auto-flush is disabled. */
    private volatile ScheduledFuture<?> flushFut;

    /** Constructor. */
    TcpClientDataStreamer(
        String cacheName,
        ReliableChannel ch,
        ClientBinaryMarshaller marsh,
        IgniteLogger log
    ) {
        this.cacheName = cacheName;
        this.cacheId = ClientUtils.cacheId(cacheName);
        this.ch = ch;
        this.serDes = new ClientUtils(marsh);
        this.log = log;

        this.allowOverwrite = false;
        this.skipStore = false;
        this.keepBinary = false;
        this.perNodeBufferSize = DFLT_PER_NODE_BUFFER_SIZE;
        this.perNodeParallelOperations = DFLT_PER_NODE_PARALLEL_OPERATIONS;
        this.timeout = DFLT_UNLIMIT_TIMEOUT;
        this.autoFlushInterval = 0;
        this.receiver = null;
        this.flags = buildFlags();

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "thin-client-ds[" + cacheName + "]");
            t.setDaemon(true);
            return t;
        });
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public boolean allowOverwrite() {
        return allowOverwrite;
    }

    /** {@inheritDoc} */
    @Override public void allowOverwrite(boolean allowOverwrite) {
        this.allowOverwrite = allowOverwrite;
        this.flags = buildFlags();
    }

    /** {@inheritDoc} */
    @Override public boolean skipStore() {
        return skipStore;
    }

    /** {@inheritDoc} */
    @Override public void skipStore(boolean skipStore) {
        this.skipStore = skipStore;
        this.flags = buildFlags();
    }

    /** {@inheritDoc} */
    @Override public boolean keepBinary() {
        return keepBinary;
    }

    /** {@inheritDoc} */
    @Override public void keepBinary(boolean keepBinary) {
        this.keepBinary = keepBinary;
        this.flags = buildFlags();
    }

    /** {@inheritDoc} */
    @Override public int perNodeBufferSize() {
        return perNodeBufferSize;
    }

    /** {@inheritDoc} */
    @Override public void perNodeBufferSize(int bufSize) {
        GridArgumentCheck.ensure(bufSize > 0, "bufSize must be > 0");
        this.perNodeBufferSize = bufSize;
    }

    /** {@inheritDoc} */
    @Override public int perNodeParallelOperations() {
        return perNodeParallelOperations;
    }

    /** {@inheritDoc} */
    @Override public void perNodeParallelOperations(int parallelOps) {
        GridArgumentCheck.ensure(parallelOps > 0, "parallelOps must be > 0");

        if (!partitions.isEmpty())
            throw new IllegalStateException("perNodeParallelOperations must be set before adding data.");

        this.perNodeParallelOperations = parallelOps;
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override public void timeout(long timeout) {
        if (timeout != TIMEOUT_DISABLED && timeout <= 0)
            throw new IllegalArgumentException("timeout must be -1 (disabled) or a positive value in milliseconds.");

        this.timeout = timeout;
    }

    /** {@inheritDoc} */
    @Override public long autoFlushFrequency() {
        return autoFlushInterval;
    }

    /** {@inheritDoc} */
    @Override public void autoFlushFrequency(long autoFlushFreq) {
        GridArgumentCheck.ensure(autoFlushFreq >= 0, "autoFlushFreq must be >= 0");

        autoFlushLock.lock();

        try {
            long autoFlushInterval0 = autoFlushInterval;
            if (autoFlushInterval0 == autoFlushFreq) {
                return;
            }

            autoFlushInterval = autoFlushFreq;

            ScheduledFuture<?> flushFut0 = flushFut;
            if (flushFut0 != null) {
                flushFut0.cancel(false);
            }

            if (autoFlushFreq == 0) {
                flushFut = null;
            } else {
                flushFut = scheduler.scheduleAtFixedRate(
                        this::tryFlush,
                        autoFlushFreq,
                        autoFlushFreq,
                        TimeUnit.MILLISECONDS
                );
            }
        } finally {
            autoFlushLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> future() {
        return new IgniteClientFutureImpl<>(closeFut);
    }

    /** {@inheritDoc} */
    @Override public void receiver(StreamReceiver<K, V> rcvr) {
        GridArgumentCheck.notNull(rcvr, "rcvr");

        this.receiver = rcvr;
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> removeData(K key) {
        GridArgumentCheck.notNull(key, "key");

        return new IgniteClientFutureImpl<>(add0(key, null));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> addData(K key, V val) {
        GridArgumentCheck.notNull(key, "key");

        return new IgniteClientFutureImpl<>(add0(key, val));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> addData(Map.Entry<K, V> entry) {
        GridArgumentCheck.notNull(entry, "entry");
        GridArgumentCheck.notNull(entry.getKey(), "entry.key");

        return new IgniteClientFutureImpl<>(add0(entry.getKey(), entry.getValue()));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> addData(Collection<? extends Map.Entry<K, V>> entries) {
        GridArgumentCheck.notEmpty(entries, "entries");

        List<CompletableFuture<Void>> futs = new ArrayList<>(entries.size());

        for (Map.Entry<K, V> e : entries) {
            GridArgumentCheck.notNull(e, "entry");
            futs.add(add0(e.getKey(), e.getValue()));
        }

        return new IgniteClientFutureImpl<>(CompletableFuture.allOf(futs.toArray(new CompletableFuture[0])));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> addData(Map<K, V> entries) {
        GridArgumentCheck.notNull(entries, "entries");

        if (entries.isEmpty())
            throw new IllegalArgumentException("entries must not be empty.");

        List<CompletableFuture<Void>> futs = new ArrayList<>(entries.size());

        for (Map.Entry<K, V> e : entries.entrySet())
            futs.add(add0(e.getKey(), e.getValue()));

        return new IgniteClientFutureImpl<>(CompletableFuture.allOf(futs.toArray(new CompletableFuture[0])));
    }

    /** {@inheritDoc} */
    @Override public void flush() {
        ensureNotClosed();

        try {
            flushAsync0().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClientException("Flush interrupted.", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw cause instanceof ClientException ? (ClientException) cause
                    : new ClientException("Flush failed.", cause);
        }
    }

    /** {@inheritDoc} */
    @Override public void tryFlush() {
        ensureNotClosed();

        flushAsync0();
    }

    /** {@inheritDoc} */
    @Override public void close(boolean cancel) {
        if (!closed.compareAndSet(false, true)) {
            // A concurrent close is already in progress; wait for it.
            try {
                closeFut.get();
            }
            catch (Exception e) {
                throw new ClientException("Close failed.", e);
            }
            return;
        }

        ScheduledFuture<?> flushFut0 = flushFut;
        if (flushFut0 != null) {
            flushFut0.cancel(cancel);
        }

        try {
            if (!cancel) {
                flushAsync0().join();
                scheduler.shutdown();
            } else {
                scheduler.shutdownNow();
            }

            scheduler.awaitTermination(1, TimeUnit.MINUTES);

            closeFut.complete(null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ClientException ce = new ClientException("Close interrupted.", e);
            closeFut.completeExceptionally(ce);
        } catch (ClientException e) {
            closeFut.completeExceptionally(e);
            throw e;
        } catch (Exception e) {
            ClientException ce = new ClientException("Close failed.", e);
            closeFut.completeExceptionally(ce);
            throw ce;
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        close(false);
    }

    /**
     * Returns {@code true} if the streamer has been closed.
     *
     * @return {@code true} if closed.
     */
    public boolean isClosed() {
        return closed.get();
    }

    /** Initiates an async send of all non-empty buffered batches and returns a future tracking their completion. */
    private CompletableFuture<Void> flushAsync0() {
        List<CompletableFuture<Void>> futs = new ArrayList<>(partitions.size());

        int bufSndSize = perNodeBufferSize;
        for (PartitionContext<K, V> ctx : partitions.values()) {
            AtomicReference<Batch<K, V>> batchRef = ctx.headBatchRef;
            Batch<K, V> cur = ctx.headBatchRef.get();

            if (!cur.isEmpty()) {
                try {
                    sendBatch(ctx, batchRef, cur, bufSndSize);
                } catch (RuntimeException e) {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    f.completeExceptionally(e);
                    return f;
                }
            }

            futs.add(ctx.pendingBatchesFuture());
        }

        return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0]));
    }

    /**
     * Internal add/remove routine.
     *
     * @param key Key.
     * @param val Value, or {@code null} to remove.
     * @return Future that completes when the batch containing this entry is sent.
     */
    private CompletableFuture<Void> add0(K key, V val) {
        int bufSndSize = perNodeBufferSize;
        @Nullable UUID nodeId = ch.resolveAffinityNode(cacheId, key);
        UUID partition = nodeId != null ? nodeId : UNKNOWN_NODE;
        PartitionContext<K, V> partCtx = partitions.computeIfAbsent(
                partition, k -> new PartitionContext<>(nodeId, perNodeParallelOperations, bufSndSize));

        AtomicReference<Batch<K, V>> batchRef = partCtx.headBatchRef;

        Batch<K, V> cur = null;
        int size = -1;
        while (size < 0) {
            ensureNotClosed();

            cur = batchRef.get();

            size = cur.add(key, val);

            if (size <= 0) {
                // Batch is either full or already in the sendQueue. Swap in a new batch and retry.
                batchRef.compareAndSet(cur, new Batch<>(bufSndSize));
            }
        }

        if (size == cur.maxSize) {
            sendBatch(partCtx, batchRef, cur, bufSndSize);
        }

        return cur.fut;
    }

    /**
     * Swaps the current batch for a fresh one and schedules an async send of the old batch.
     *
     * @param ctx        Partition context owning the batch.
     * @param batchRef   Per-partition batch reference to swap.
     * @param batch      Batch to seal and send.
     * @param bufSndSize Capacity for the replacement batch.
     */
    private void sendBatch(PartitionContext<K, V> ctx, AtomicReference<Batch<K, V>> batchRef, Batch<K, V> batch, int bufSndSize) {
        batchRef.compareAndSet(batch, new Batch<>(bufSndSize));
        if (batch.lockForSend()) {
            ctx.pendingBatches.add(batch);
            try {
                scheduler.submit(() -> sendToChannel(ctx));
            } catch (RejectedExecutionException e) {
                throw new ClientException("Failed to trigger send batch.", e);
            }
        }
    }

    /**
     * Sends {@code entries} to the server via a single {@code DATA_STREAMER_START} request
     * with {@code FLUSH | CLOSE} flags.
     *
     * <p>When {@code ctx.nodeId} is not {@code null}, the request is routed directly to that
     * cluster node; otherwise the default channel is used.</p>
     *
     * <p>Wire format (matches {@code ClientDataStreamerStartRequest} on the server side):</p>
     * <pre>
     *   cacheId          (int)
     *   flags            (byte)  ALLOW_OVERWRITE=0x01 | SKIP_STORE=0x02 | KEEP_BINARY=0x04 | FLUSH=0x08 | CLOSE=0x10
     *   perNodeBufSize   (int)   -1 (auto)
     *   perThreadBufSize (int)   -1 (auto)
     *   receiverObj      (Object) binary-encoded receiver, or null
     *   [receiverPlatform (byte)] ClientPlatform.JAVA=1, only present when receiverObj != null
     *   entriesCnt       (int)
     *   [key (Object), val (Object)]...  val == null means remove
     * </pre>
     *
     * @param ctx Partition context holding the batch queue and inflight slots.
     */
    void sendToChannel(PartitionContext<K, V> ctx) {
        int slot = ctx.trySendSlot();
        if (slot < 0) {
            return;
        }

        Batch<K, V> cur = ctx.inflightBatches.get(slot);

        long timeout0 = timeout;
        if (timeout0 != TIMEOUT_DISABLED) {
            long elapsed = System.nanoTime() - cur.sndTimestamp;
            if (elapsed > TimeUnit.MILLISECONDS.toNanos(timeout0)) {
                finishBatchSend(
                        ctx,
                        slot,
                        cur,
                        () -> new ClientException("Timed out waiting for a send permit.")
                );
                return;
            }
        }

        // Actual send batch.
        StreamReceiver<K, V> rcv = receiver;
        byte f = flags;
        UUID nodeId = ctx.nodeId;

        Consumer<PayloadOutputChannel> payloadWriter = req -> {
            BinaryOutputStream out = req.out();

            out.writeInt(cacheId);
            out.writeByte(f);
            out.writeInt(SERVER_BUFFER_SIZE_AUTO);  // perNodeBufferSize
            out.writeInt(SERVER_BUFFER_SIZE_AUTO);  // perThreadBufferSize

            serDes.writeObject(out, rcv);
            if (rcv != null)
                out.writeByte(ClientPlatform.JAVA);

            // Will also ensure visibility
            int size = Math.min(cur.size.get(), cur.maxSize);
            out.writeInt(size);

            size *= 2;
            for (int i = 0; i < size; i++) {
                serDes.writeObject(out, cur.elements[i]);
            }
        };

        Function<PayloadInputChannel, Long> payloadReader = res -> res.in().readLong();

        ch.nodeServiceAsync(
                nodeId,
                ClientOperation.DATA_STREAMER_START,
                payloadWriter,
                payloadReader
        ).whenComplete((res, err) -> {
            finishBatchSend(
                    ctx,
                    slot,
                    cur,
                    () -> err == null ? null : new ClientException("Data streamer batch send failed.", err)
            );
        });
    }

    private void finishBatchSend(
            PartitionContext<K, V> ctx,
            int slot,
            Batch<K, V> batch,
            Supplier<@Nullable Throwable> errSupplier
    ) {
        ctx.inflightBatches.set(slot, null);
        try {
            scheduler.submit(() -> sendToChannel(ctx));
        } catch (RejectedExecutionException e) {
            log.error("Data streamer scheduler rejected batch send, streamer may be closing [cache=" + cacheName + ']', e);
        }

        @Nullable Throwable err = errSupplier.get();
        if (err != null) {
            batch.fut.completeExceptionally(err);
        } else {
            batch.fut.complete(null);
        }
    }

    private void ensureNotClosed() throws ClientException {
        if (closed.get())
            throw new ClientException("Data streamer is closed.");
    }

    /**
     * Builds the flags byte from the current option fields.
     * Always includes {@code FLUSH} and {@code CLOSE} so that each batch is
     * a self-contained one-shot operation with no persistent server-side state.
     */
    private byte buildFlags() {
        byte f = FLUSH | CLOSE;

        if (allowOverwrite) f |= ALLOW_OVERWRITE;
        if (skipStore) f |= SKIP_STORE;
        if (keepBinary) f |= KEEP_BINARY;

        return f;
    }

    /**
     * Accumulates entries for a single {@code DATA_STREAMER_START} send.
     */
    private static class Batch<K, V> {
        /** Maximum number of entries this batch can hold. */
        final int maxSize;

        /** Flat key/value storage: {@code elements[2i]} = key, {@code elements[2i+1]} = value. */
        final Object[] elements;

        /** Logical entry count. */
        private final AtomicInteger size = new AtomicInteger(0);

        /**
         * Nanosecond timestamp recorded when the batch is sealed, or {@code -1} if not yet sealed.
         * A value {@code > 0} means the batch is sealed; used by the timeout check in
         * {@link TcpClientDataStreamer#sendToChannel}.
         */
        private volatile long sndTimestamp = Long.MIN_VALUE;

        /**
         * Read lock allows concurrent {@link #add} calls; write lock is held only during
         * {@link #lockForSend} to atomically seal the batch.
         */
        private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

        /** Completes when this batch's send is acknowledged (or when the batch was empty). */
        final CompletableFuture<Void> fut = new CompletableFuture<>();

        /** Constructor. */
        Batch(int size) {
            this.maxSize = size;
            this.elements = new Object[2 * size];
        }

        /**
         * Tries to add an entry.
         *
         * @return New logical size, or {@code -1} if the batch is already sealed.
         */
        int add(K key, V val) {
            // Non-blocking: if the write lock is held (send in progress), the CAS in add0 swaps a new batch.
            if (!rwLock.readLock().tryLock())
                return -1;

            try {
                if (sndTimestamp != Long.MIN_VALUE)
                    return -1; // Closed.

                int nelems = size.getAndIncrement();
                int idx = nelems * 2;
                if (idx >= elements.length) {
                    return -2; // Full.
                }

                elements[idx] = key;
                elements[idx + 1] = val; // May be null for removes.

                return nelems + 1;
            }
            finally {
                rwLock.readLock().unlock();
            }
        }

        /** Returns {@code true} when no entries have been added to this batch. */
        boolean isEmpty() {
            return size.get() == 0;
        }

        /**
         * Seals this batch so no more entries can be added.
         *
         * @return {@code true} if this call successfully sealed the batch; {@code false} if already sealed or empty.
         */
        boolean lockForSend() {
            // Seal: write lock excludes concurrent adders, then set the guard.
            rwLock.writeLock().lock();

            try {
                if (sndTimestamp != Long.MIN_VALUE)
                    return false;  // Another thread already triggered the send.

                if (isEmpty())
                    return false;

                sndTimestamp = System.nanoTime();
                return true;
            }
            finally {
                rwLock.writeLock().unlock();
            }
        }
    }

    /** Manages sending of batches for a single cluster node. */
    private static class PartitionContext<K, V> {
        /**
         * Primary node UUID for all keys in this context, or {@code null} when partition
         * awareness is disabled or the affinity mapping was not yet available.
         * A {@code null} value causes sends to fall back to the default channel.
         */
        @Nullable final UUID nodeId;

        /** Batches that have been sealed and are waiting for a free inflight slot. */
        final BlockingQueue<Batch<K, V>> pendingBatches = new LinkedBlockingQueue<>();

        /** The batch currently accepting new entries. */
        final AtomicReference<Batch<K, V>> headBatchRef;

        /**
         * Slots for batches actively being sent to the server.
         * Length equals {@code perNodeParallelOperations}.
         */
        final AtomicReferenceArray<Batch<K, V>> inflightBatches;

        /**
         * @param nodeId             Primary node UUID, or {@code null} when the node is unknown.
         * @param perNodeParallelOps Maximum number of concurrently in-flight batches.
         * @param batchSize          Capacity of each new {@link Batch}.
         */
        PartitionContext(@Nullable UUID nodeId, int perNodeParallelOps, int batchSize) {
            this.nodeId = nodeId;
            this.headBatchRef = new AtomicReference<>(new Batch<>(batchSize));
            this.inflightBatches = new AtomicReferenceArray<>(perNodeParallelOps);
        }

        /**
         * Attempts to move the next pending batch into a free inflight slot.
         *
         * @return The slot index used, or {@code -1} if there are no pending batches or no free slots.
         */
        int trySendSlot() {
            Batch<K, V> batch = pendingBatches.peek();
            if (batch == null) {
                return -1;
            }

            for (int i = 0; i < inflightBatches.length(); i++) {
                boolean foundEmptySlot = inflightBatches.compareAndSet(i, null, batch);
                if (foundEmptySlot) {
                    pendingBatches.remove(batch);
                    return i;
                }
            }

            return -1;
        }

        /**
         * Returns a future that completes when all currently pending and inflight batches have been acknowledged.
         */
        CompletableFuture<Void> pendingBatchesFuture() {
            List<CompletableFuture<Void>> futs = new ArrayList<>();

            for (Batch<K, V> batch : pendingBatches) {
                futs.add(batch.fut);
            }

            for (int i = 0; i < inflightBatches.length(); i++) {
                Batch<K, V> batch = inflightBatches.get(i);
                if (batch != null) {
                    futs.add(batch.fut);
                }
            }

            return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0]));
        }
    }
}

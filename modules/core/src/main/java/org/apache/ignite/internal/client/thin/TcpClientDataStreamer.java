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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.client.datastreamer.ClientDataStreamer;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.processors.platform.client.ClientPlatform;
import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.stream.StreamReceiver;

import static org.apache.ignite.IgniteDataStreamer.*;
import static org.apache.ignite.client.datastreamer.ClientDataStreamer.DFLT_PER_NODE_PARALLEL_OPERATIONS;
import static org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerFlags.ALLOW_OVERWRITE;
import static org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerFlags.CLOSE;
import static org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerFlags.FLUSH;
import static org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerFlags.KEEP_BINARY;
import static org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerFlags.SKIP_STORE;

/**
 * Implementation of {@link ClientDataStreamer} over TCP protocol.
 *
 * <p>Each batch is sent as a self-contained {@code DATA_STREAMER_START} request with
 * {@code FLUSH | CLOSE} flags, matching the behaviour of the .NET thin client.</p>
 *
 * <p>When partition awareness is enabled, a separate batch chain is maintained per cache partition
 * so that each batch is sent directly to the node that owns those partitions. When partition
 * awareness is disabled or the affinity mapping is not yet available, all entries share a single
 * chain keyed by {@link ClientCacheAffinityMapping#UNKNOWN_PARTITION}.</p>
 */
class TcpClientDataStreamer<K, V> implements ClientDataStreamer<K, V> {

    /**
     * Sentinel value passed as perNodeBufferSize / perThreadBufferSize to let the server
     * use its own defaults.
     */
    private static final int SERVER_BUFFER_SIZE_AUTO = -1;

    /** Sentinel for disabled timeout (matches {@code IgniteDataStreamer.DFLT_UNLIMIT_TIMEOUT}). */
    private static final long TIMEOUT_DISABLED = -1L;

    /** Cache name. */
    private final String cacheName;

    /** Cache ID. */
    private final int cacheId;

    /** Channel. */
    private final ReliableChannel ch;

    /** Serializer/deserializer. */
    private final ClientUtils serDes;

    private volatile boolean allowOverwrite;

    private volatile boolean skipStore;

    /** keepBinary flag for the stream receiver. */
    private volatile boolean keepBinary;

    private volatile int perNodeBufferSize;

    private volatile int perNodeParallelOperations;

    /** Timeout in milliseconds; {@link #TIMEOUT_DISABLED} means unlimited. */
    private volatile long timeout;

    /** Auto-flush interval in milliseconds; 0 means disabled. */
    private volatile long autoFlushInterval;

    private volatile StreamReceiver<K, V> receiver;

    /**
     * Flags byte derived from {@link #allowOverwrite}, {@link #skipStore}, {@link #keepBinary}.
     * Always includes {@code FLUSH | CLOSE} for one-shot semantics.
     */
    private volatile byte flags;

    /**
     * Per-partition batch chains. Key: cache partition index, or
     * {@link ClientCacheAffinityMapping#UNKNOWN_PARTITION} when partition awareness is
     * disabled or the affinity mapping is not yet available for the entry's key.
     */
    private final ConcurrentHashMap<Integer, AtomicReference<Batch<K, V>>> partitionBatches =
        new ConcurrentHashMap<>();

    /** Set to {@code true} once {@link #close(boolean)} has been called. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** Limits the number of in-flight batch sends. Replaced when {@link #perNodeParallelOperations(int)} is called. */
    private volatile Semaphore flightSem;

    /** Completed when {@link #close(boolean)} finishes. */
    private final CompletableFuture<Void> closeFut = new CompletableFuture<>();

    /** Background auto-flusher. */
    private final Flusher flusher;

    /** Constructor. */
    TcpClientDataStreamer(
        String cacheName,
        ReliableChannel ch,
        ClientBinaryMarshaller marsh
    ) {
        this.cacheName = cacheName;
        this.cacheId = ClientUtils.cacheId(cacheName);
        this.ch = ch;
        this.serDes = new ClientUtils(marsh);

        this.allowOverwrite = false;
        this.skipStore = false;
        this.keepBinary = false;
        this.perNodeBufferSize = DFLT_PER_NODE_BUFFER_SIZE;
        this.perNodeParallelOperations = DFLT_PER_NODE_PARALLEL_OPERATIONS;
        this.timeout = DFLT_UNLIMIT_TIMEOUT;
        this.autoFlushInterval = 0;
        this.receiver = null;
        this.flags = buildFlags();

        this.flightSem = new Semaphore(this.perNodeParallelOperations);
        this.flusher = new Flusher();
        this.flusher.start();
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

        if (!partitionBatches.isEmpty())
            throw new IllegalStateException("perNodeParallelOperations must be set before adding data.");

        this.perNodeParallelOperations = parallelOps;
        this.flightSem = new Semaphore(parallelOps);
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

        this.autoFlushInterval = autoFlushFreq;

        // Wake the flusher so it picks up the new interval on its next loop iteration.
        synchronized (flusher) {
            flusher.notifyAll();
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
        try {
            flushAsync().get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClientException("Flush interrupted.", e);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw cause instanceof ClientException ? (ClientException) cause
                : new ClientException("Flush failed.", cause);
        }
    }

    /** {@inheritDoc} */
    @Override public void tryFlush() {
        for (AtomicReference<Batch<K, V>> batchRef : partitionBatches.values()) {
            Batch<K, V> cur = batchRef.get();

            if (cur != null && !cur.isEmpty())
                sendBatch(batchRef, cur);
        }
    }

    /** {@inheritDoc} */
    @Override public void close(boolean cancel) {
        flusher.stopFlusher();

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

        try {
            if (!cancel) {
                List<CompletableFuture<Void>> futs = new ArrayList<>();

                for (AtomicReference<Batch<K, V>> batchRef : partitionBatches.values()) {
                    Batch<K, V> cur = batchRef.get();

                    if (cur != null) {
                        cur.send(this);
                        futs.add(cur.allDoneFuture());
                    }
                }

                try {
                    CompletableFuture.allOf(futs.toArray(new CompletableFuture[0])).get();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ClientException("Close interrupted.", e);
                }
                catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    throw cause instanceof ClientException ? (ClientException) cause
                        : new ClientException("Close failed.", cause);
                }
            }

            closeFut.complete(null);
        }
        catch (ClientException e) {
            closeFut.completeExceptionally(e);
            throw e;
        }
        catch (Exception e) {
            ClientException ce = new ClientException("Close failed.", e);
            closeFut.completeExceptionally(ce);
            throw ce;
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        close(false);
    }

    /** Returns {@code true} if the streamer has been closed. */
    public boolean isClosed() {
        return closed.get();
    }

    /** Flushes all buffered data asynchronously. */
    public IgniteClientFuture<Void> flushAsync() {
        if (closed.get())
            return new IgniteClientFutureImpl<>(closeFut);

        List<CompletableFuture<Void>> futs = new ArrayList<>();

        for (AtomicReference<Batch<K, V>> batchRef : partitionBatches.values()) {
            Batch<K, V> cur = batchRef.get();

            if (cur != null) {
                sendBatch(batchRef, cur);
                futs.add(cur.allDoneFuture());
            }
        }

        if (futs.isEmpty())
            return new IgniteClientFutureImpl<>(CompletableFuture.completedFuture(null));

        return new IgniteClientFutureImpl<>(
            CompletableFuture.allOf(futs.toArray(new CompletableFuture[0])));
    }

    /** Closes this data streamer asynchronously. */
    public IgniteClientFuture<Void> closeAsync(boolean cancel) {
        return new IgniteClientFutureImpl<>(CompletableFuture.runAsync(() -> close(cancel)));
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
        int partition = ch.resolveAffinityPartition(cacheId, key);
        AtomicReference<Batch<K, V>> batchRef = partitionBatches.computeIfAbsent(
            partition, k -> new AtomicReference<>(new Batch<>(partition, null)));

        while (true) {
            if (closed.get())
                throw new ClientException("Data streamer is stopped.");

            Batch<K, V> cur = batchRef.get();

            int size = cur.add(key, val);

            if (size == -1) {
                // Batch is sealed (send in progress); swap in a fresh one and retry.
                batchRef.compareAndSet(cur, new Batch<>(partition, cur));
                continue;
            }

            if (size >= bufSndSize)
                sendBatch(batchRef, cur);

            return cur.fut;
        }
    }

    /**
     * Swaps the current batch for a fresh one and initiates an async send of the old batch.
     *
     * @param batchRef Per-partition batch reference.
     * @param batch    Batch to send.
     */
    private void sendBatch(AtomicReference<Batch<K, V>> batchRef, Batch<K, V> batch) {
        batchRef.compareAndSet(batch, new Batch<>(batch.partition, batch));
        batch.send(this);
    }

    /**
     * Sends {@code entries} to the server via a single {@code DATA_STREAMER_START} request
     * with {@code FLUSH | CLOSE} flags, matching the one-shot approach of the .NET thin client.
     *
     * <p>When {@code partition} is not {@link ClientCacheAffinityMapping#UNKNOWN_PARTITION}, the
     * request is routed to the node owning that partition; otherwise the default channel is used.</p>
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
     * @param partition Cache partition owning these entries, or {@link ClientCacheAffinityMapping#UNKNOWN_PARTITION}.
     * @param entries   List of {@code [key, val]} pairs; {@code null} val means remove.
     * @param result    Future to complete on success or failure.
     */
    void sendToChannel(int partition, List<Object[]> entries, CompletableFuture<Void> result) {
        // Capture volatile fields as locals for consistent use within this send operation.
        long timeoutMs = timeout;
        final Semaphore sem = flightSem;

        try {
            if (timeoutMs == TIMEOUT_DISABLED)
                sem.acquire();
            else if (!sem.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
                result.completeExceptionally(new ClientException("Timed out waiting for a send permit."));
                return;
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result.completeExceptionally(new ClientException("Interrupted waiting for send slot.", e));
            return;
        }

        try {
            StreamReceiver<K, V> rcv = receiver;
            byte f = flags;

            Consumer<PayloadOutputChannel> payloadWriter = req -> {
                BinaryOutputStream out = req.out();

                out.writeInt(cacheId);
                out.writeByte(f);
                out.writeInt(SERVER_BUFFER_SIZE_AUTO);  // perNodeBufferSize
                out.writeInt(SERVER_BUFFER_SIZE_AUTO);  // perThreadBufferSize

                serDes.writeObject(out, rcv);
                if (rcv != null)
                    out.writeByte(ClientPlatform.JAVA);

                out.writeInt(entries.size());

                for (Object[] e : entries) {
                    serDes.writeObject(out, e[0]);  // key
                    serDes.writeObject(out, e[1]);  // value (null = remove)
                }
            };

            Function<PayloadInputChannel, Long> payloadReader = res -> res.in().readLong();

            ch.affinityServiceAsync(
                    cacheId,
                    partition,
                    ClientOperation.DATA_STREAMER_START,
                    payloadWriter,
                    payloadReader
            ).whenComplete((res, err) -> {
                sem.release();

                if (err != null)
                    result.completeExceptionally(new ClientException("Data streamer batch send failed.", err));
                else
                    result.complete(null);
            });
        }
        catch (Exception e) {
            sem.release();
            result.completeExceptionally(e);
        }
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
     * Batches are chained via {@link #prev} so that completion can be tracked across
     * a series of flushes.
     */
    static final class Batch<K, V> {

        /** Cache partition this batch will be sent to (or {@link ClientCacheAffinityMapping#UNKNOWN_PARTITION}). */
        final int partition;

        /** Entries: each element is {@code Object[]{key, val}}; {@code null} val means remove. */
        private final ConcurrentLinkedQueue<Object[]> queue = new ConcurrentLinkedQueue<>();

        /** Logical entry count. */
        private final AtomicInteger size = new AtomicInteger();

        /** Once {@code true}, no more entries can be added to this batch. */
        private volatile boolean sndGuard;

        /**
         * Guards {@link #sndGuard}: tryLock(0) on the read side allows concurrent adds;
         * write lock seals the batch before sending.
         */
        private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

        /** Previous batch in the chain. Released once this and the previous batch complete. */
        private volatile Batch<K, V> prev;

        /** Completes when this batch's send is acknowledged (or when the batch was empty). */
        final CompletableFuture<Void> fut = new CompletableFuture<>();

        /** Constructor. */
        Batch(int partition, Batch<K, V> prev) {
            this.partition = partition;
            this.prev = prev;
            // Release the prev link once this batch completes to avoid accumulating large chains.
            fut.thenRun(this::tryReleasePrev);
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
                if (sndGuard)
                    return -1;

                queue.offer(new Object[]{key, val});
                return size.incrementAndGet();
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
         * Seals this batch (and all previous batches first) and initiates an async send.
         *
         * @param streamer Parent streamer used to perform the channel write.
         */
        void send(TcpClientDataStreamer<K, V> streamer) {
            // Recursively flush older batches first to preserve entry ordering.
            Batch<K, V> prev0 = prev;

            if (prev0 != null)
                prev0.send(streamer);

            // Seal: write lock excludes concurrent adders, then set the guard.
            rwLock.writeLock().lock();

            try {
                if (sndGuard)
                    return;  // Another thread already triggered the send.

                sndGuard = true;
            }
            finally {
                rwLock.writeLock().unlock();
            }

            // Drain the queue into a snapshot list.
            List<Object[]> entries = new ArrayList<>(size.get());
            Object[] e;

            while ((e = queue.poll()) != null)
                entries.add(e);

            if (entries.isEmpty())
                fut.complete(null);
            else
                streamer.sendToChannel(partition, entries, fut);
        }

        /**
         * Returns a future that completes when this and all preceding batches have been sent.
         */
        CompletableFuture<Void> allDoneFuture() {
            List<CompletableFuture<Void>> futs = new ArrayList<>();

            for (Batch<K, V> cur = this; cur != null; cur = cur.prev) {
                if (!cur.fut.isDone())
                    futs.add(cur.fut);
            }

            return futs.isEmpty()
                ? CompletableFuture.completedFuture(null)
                : CompletableFuture.allOf(futs.toArray(new CompletableFuture[0]));
        }

        /** Drops the prev link once the chain up to this batch is fully complete. */
        private void tryReleasePrev() {
            Batch<K, V> prev0 = prev;

            if (prev0 != null && prev0.fut.isDone())
                prev = null;
        }
    }

    /**
     * Daemon thread that periodically triggers {@link TcpClientDataStreamer#tryFlush()}.
     * Sleeps for {@link #autoFlushInterval} milliseconds between flushes; when the interval
     * is zero it only idles, waking solely on the stop signal.
     */
    private final class Flusher extends Thread {
        // TODO: Check if there is a routine capable of doing this instead of a separate thread.

        /** State: running normally. */
        private static final int STATE_RUNNING = 0;

        /** State: stop requested. */
        private static final int STATE_STOPPING = 1;

        /** State: fully stopped. */
        private static final int STATE_STOPPED = 2;

        /** Current state. */
        private volatile int state = STATE_RUNNING;

        /** Constructor. */
        Flusher() {
            super("thin-client-ds-flusher[" + cacheName + "]");
            setDaemon(true);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                while (true) {
                    long freq = autoFlushInterval;

                    synchronized (this) {
                        if (state == STATE_STOPPING)
                            break;

                        try {
                            wait(freq > 0 ? freq : 1_000L);
                        }
                        catch (InterruptedException e) {
                            break;
                        }

                        if (state == STATE_STOPPING)
                            break;
                    }

                    if (freq > 0)
                        tryFlush();
                }
            }
            finally {
                synchronized (this) {
                    state = STATE_STOPPED;
                    notifyAll();
                }
            }
        }

        /** Signals the flusher to stop and waits for it to exit. */
        synchronized void stopFlusher() {
            if (state == STATE_RUNNING) {
                state = STATE_STOPPING;
                notifyAll();
            }

            while (state != STATE_STOPPED) {
                try {
                    wait();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}

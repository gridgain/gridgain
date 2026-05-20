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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.client.datastreamer.ClientDataStreamer;
import org.apache.ignite.client.datastreamer.DataStreamerClientOptions;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.processors.platform.client.ClientPlatform;
import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.stream.StreamReceiver;

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
 */
class TcpClientDataStreamer<K, V> implements ClientDataStreamer<K, V> {

    /**
     * Sentinel value passed as perNodeBufferSize / perThreadBufferSize to let the server
     * use its own defaults (mirrors .NET {@code ServerBufferSizeAuto = -1}).
     */
    private static final int SERVER_BUFFER_SIZE_AUTO = -1;

    /** Cache name. */
    private final String cacheName;

    /** Cache ID. */
    private final int cacheId;

    /** Channel. */
    private final ReliableChannel ch;

    /** Serializer/deserializer. */
    private final ClientUtils serDes;

    /** Options (immutable snapshot taken at construction). */
    private final DataStreamerClientOptions<K, V> opts;

    /**
     * Flags byte pre-computed at construction and reused for every batch.
     * Always includes {@code FLUSH} and {@code CLOSE} so that every
     * {@code DATA_STREAMER_START} request is a self-contained one-shot operation.
     */
    private final byte flags;

    /**
     * Reference to the active batch.
     * {@code null} once the streamer is closed.
     */
    private final AtomicReference<Batch<K, V>> batchRef;

    /** Limits the number of in-flight batch sends. */
    private final Semaphore flightSem;

    /** Completed when {@link #close(boolean)} finishes. */
    private final CompletableFuture<Void> closeFut = new CompletableFuture<>();

    /** Background auto-flusher. */
    private final Flusher flusher;

    /** Constructor. */
    TcpClientDataStreamer(
        String cacheName,
        ReliableChannel ch,
        ClientBinaryMarshaller marsh,
        DataStreamerClientOptions<K, V> opts
    ) {
        this.cacheName = cacheName;
        this.cacheId = ClientUtils.cacheId(cacheName);
        this.ch = ch;
        this.serDes = new ClientUtils(marsh);
        this.opts = opts;
        this.flags = buildFlags(opts);
        this.batchRef = new AtomicReference<>(new Batch<>(null));
        this.flightSem = new Semaphore(opts.getPerNodeParallelOperations());
        this.flusher = new Flusher();
        this.flusher.start();
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return batchRef.get() == null;
    }

    /** {@inheritDoc} */
    @Override public DataStreamerClientOptions<K, V> options() {
        return opts;
    }

    /** {@inheritDoc} */
    @Override public void add(K key, V val) {
        GridArgumentCheck.notNull(key, "key");

        add0(key, val);
    }

    /** {@inheritDoc} */
    @Override public void remove(K key) {
        GridArgumentCheck.notNull(key, "key");

        if (!opts.isAllowOverwrite())
            throw new ClientException("DataStreamer can't remove data when AllowOverwrite is false.");

        add0(key, null);
    }

    /**
     * Internal add/remove routine.
     *
     * @param key Key.
     * @param val Value, or {@code null} to remove.
     */
    private void add0(K key, V val) {
        // TODO: Probably rename.
        int bufSndSize = opts.getPerNodeBufferSize();

        while (true) {
            Batch<K, V> cur = batchRef.get();

            if (cur == null)
                throw new ClientException("Data streamer is stopped.");

            int size = cur.add(key, val);

            if (size == -1) {
                // Batch is sealed (send in progress); swap in a fresh one and retry.
                batchRef.compareAndSet(cur, new Batch<>(cur));
                continue;
            }

            if (size >= bufSndSize)
                sendBatch(cur);

            return;
        }
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
    @Override public IgniteClientFuture<Void> flushAsync() {
        Batch<K, V> cur = batchRef.get();

        if (cur != null) {
            sendBatch(cur);
            return new IgniteClientFutureImpl<>(cur.allDoneFuture());
        }

        return new IgniteClientFutureImpl<>(closeFut);
    }

    /** {@inheritDoc} */
    @Override public void close(boolean cancel) {
        flusher.stopFlusher();

        while (true) {
            Batch<K, V> cur = batchRef.get();

            if (cur == null) {
                // A concurrent close is already in progress; wait for it.
                try {
                    closeFut.get();
                }
                catch (Exception e) {
                    throw new ClientException("Close failed.", e);
                }
                return;
            }

            // Atomically mark the streamer as closed so no new entries can be added.
            if (!batchRef.compareAndSet(cur, null))
                continue;

            try {
                if (!cancel) {
                    cur.send(this);

                    try {
                        cur.allDoneFuture().get();
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

            return;
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> closeAsync(boolean cancel) {
        return new IgniteClientFutureImpl<>(CompletableFuture.runAsync(() -> close(cancel)));
    }

    /**
     * Swaps the current batch for a fresh one and initiates an async send of the old batch.
     *
     * @param batch Batch to send.
     */
    private void sendBatch(Batch<K, V> batch) {
        batchRef.compareAndSet(batch, new Batch<>(batch));
        batch.send(this);
    }

    /** Flushes the current batch if it is non-empty. Called by the auto-flusher thread. */
    private void tryFlush() {
        Batch<K, V> cur = batchRef.get();

        if (cur != null && !cur.isEmpty())
            sendBatch(cur);
    }

    /**
     * Sends {@code entries} to the server via a single {@code DATA_STREAMER_START} request
     * with {@code FLUSH | CLOSE} flags, matching the one-shot approach of the .NET thin client.
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
     * @param entries List of {@code [key, val]} pairs; {@code null} val means remove.
     * @param result  Future to complete on success or failure.
     */
    void sendToChannel(List<Object[]> entries, CompletableFuture<Void> result) {
        try {
            flightSem.acquire();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result.completeExceptionally(new ClientException("Interrupted waiting for send slot.", e));
            return;
        }

        try {
            StreamReceiver<K, V> rcv = opts.getReceiver();

            ch.serviceAsync(ClientOperation.DATA_STREAMER_START, req -> {
                BinaryOutputStream out = req.out();

                out.writeInt(cacheId);
                out.writeByte(flags);
                out.writeInt(SERVER_BUFFER_SIZE_AUTO);  // perNodeBufferSize
                out.writeInt(SERVER_BUFFER_SIZE_AUTO);  // perThreadBufferSize

                // Receiver: write the object; if non-null, follow with the platform code.
                serDes.writeObject(out, rcv);
                if (rcv != null)
                    out.writeByte(ClientPlatform.JAVA);

                out.writeInt(entries.size());

                for (Object[] e : entries) {
                    serDes.writeObject(out, e[0]);  // key
                    serDes.writeObject(out, e[1]);  // value (null = remove)
                }
            }, res -> res.in().readLong()).whenComplete((res, err) -> {
                flightSem.release();

                if (err != null)
                    result.completeExceptionally(new ClientException("Data streamer batch send failed.", err));
                else
                    result.complete(null);
            });
        }
        catch (Exception e) {
            flightSem.release();
            result.completeExceptionally(e);
        }
    }

    /**
     * Builds the flags byte from the options.
     * Always includes {@code FLUSH} and {@code CLOSE} so that each batch is
     * a self-contained one-shot operation with no persistent server-side state.
     */
    private static byte buildFlags(DataStreamerClientOptions<?, ?> opts) {
        byte f = FLUSH | CLOSE;

        if (opts.isAllowOverwrite())     f |= ALLOW_OVERWRITE;
        if (opts.isSkipStore())          f |= SKIP_STORE;
        if (opts.isReceiverKeepBinary()) f |= KEEP_BINARY;

        return f;
    }

    /**
     * Accumulates entries for a single {@code DATA_STREAMER_START} send.
     * Batches are chained via {@link #prev} so that completion can be tracked across
     * a series of flushes.
     */
    static final class Batch<K, V> {

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
        private final CompletableFuture<Void> fut = new CompletableFuture<>();

        /** Constructor. */
        Batch(Batch<K, V> prev) {
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
                streamer.sendToChannel(entries, fut);
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
     * Sleeps for {@link DataStreamerClientOptions#getAutoFlushInterval()} milliseconds between
     * flushes; when the interval is zero it only idles, waking solely on the stop signal.
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
                    long freq = opts.getAutoFlushInterval();

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

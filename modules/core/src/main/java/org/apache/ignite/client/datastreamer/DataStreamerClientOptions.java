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

package org.apache.ignite.client.datastreamer;

import org.apache.ignite.stream.StreamReceiver;

// TODO: Reconcile with the full datastreamer.
/**
 * Thin client data streamer options.
 * <p>
 * See also {@link ClientDataStreamer}.
 */
public class DataStreamerClientOptions<K, V> {
    /** Default client-side per-node buffer size (cache entries count). */
    public static final int DFLT_PER_NODE_BUFFER_SIZE = 512;

    /** Default limit for parallel operations per server node connection. Calculated as processor count times 4. */
    public static final int DFLT_PER_NODE_PARALLEL_OPERATIONS = Runtime.getRuntime().availableProcessors() * 4;

    /** Per-node buffer size. */
    private int perNodeBufSize = DFLT_PER_NODE_BUFFER_SIZE;

    /** Per-node parallel operations limit. */
    private int perNodeParallelOps = DFLT_PER_NODE_PARALLEL_OPERATIONS;

    /** Auto flush interval in milliseconds. 0 means disabled. */
    private long autoFlushIntervalMs = 0;

    /** Allow overwrite flag. */
    private boolean allowOverwrite;

    /** Skip store flag. */
    private boolean skipStore;

    /** Keep binary flag for the receiver. */
    private boolean receiverKeepBinary;

    /** Custom stream receiver. */
    private StreamReceiver<K, V> receiver;

    /**
     * Gets the size (entry count) of per-node buffer.
     * <p>
     * Default is {@link #DFLT_PER_NODE_BUFFER_SIZE}.
     *
     * @return Per-node buffer size.
     */
    public int getPerNodeBufferSize() {
        return perNodeBufSize;
    }

    /**
     * Sets the size (entry count) of per-node buffer.
     * <p>
     * Default is {@link #DFLT_PER_NODE_BUFFER_SIZE}.
     *
     * @param perNodeBufSize Per-node buffer size. Must be greater than 0.
     * @return {@code this} for chaining.
     */
    public DataStreamerClientOptions<K, V> setPerNodeBufferSize(int perNodeBufSize) {
        if (perNodeBufSize <= 0)
            throw new IllegalArgumentException("perNodeBufSize must be > 0");

        this.perNodeBufSize = perNodeBufSize;

        return this;
    }

    /**
     * Gets the limit for parallel operations per server node.
     * <p>
     * Default is {@link #DFLT_PER_NODE_PARALLEL_OPERATIONS}.
     *
     * @return Per-node parallel operations limit.
     */
    public int getPerNodeParallelOperations() {
        return perNodeParallelOps;
    }

    /**
     * Sets the limit for parallel operations per server node.
     * <p>
     * Default is {@link #DFLT_PER_NODE_PARALLEL_OPERATIONS}.
     *
     * @param perNodeParallelOps Per-node parallel operations limit. Must be greater than 0.
     * @return {@code this} for chaining.
     */
    public DataStreamerClientOptions<K, V> setPerNodeParallelOperations(int perNodeParallelOps) {
        if (perNodeParallelOps <= 0)
            throw new IllegalArgumentException("perNodeParallelOps must be > 0");

        this.perNodeParallelOps = perNodeParallelOps;

        return this;
    }

    /**
     * Gets the automatic flush interval in milliseconds. Data streamer buffers the data for performance reasons.
     * The buffer is flushed in the following cases:
     * <ul>
     * <li>Buffer is full.</li>
     * <li>{@link ClientDataStreamer#flush()} is called.</li>
     * <li>Periodically when auto flush interval is set.</li>
     * </ul>
     * <p>
     * When set to {@code 0}, automatic flush is disabled.
     * <p>
     * Default is {@code 0} (disabled).
     *
     * @return Auto flush interval in milliseconds.
     */
    public long getAutoFlushInterval() {
        return autoFlushIntervalMs;
    }

    /**
     * Sets the automatic flush interval in milliseconds.
     * <p>
     * When set to {@code 0}, automatic flush is disabled.
     * <p>
     * Default is {@code 0} (disabled).
     *
     * @param autoFlushIntervalMs Auto flush interval in milliseconds. Must be >= 0.
     * @return {@code this} for chaining.
     */
    public DataStreamerClientOptions<K, V> setAutoFlushInterval(long autoFlushIntervalMs) {
        if (autoFlushIntervalMs < 0)
            throw new IllegalArgumentException("autoFlushIntervalMs must be >= 0");

        this.autoFlushIntervalMs = autoFlushIntervalMs;

        return this;
    }

    /**
     * Gets a value indicating whether existing values can be overwritten by the data streamer.
     * Performance is better when this flag is {@code false}.
     * <p>
     * NOTE: When {@code false}, cache updates won't be propagated to cache store
     * (even if {@link #isSkipStore()} is {@code false}).
     * <p>
     * Default is {@code false}.
     *
     * @return Allow overwrite flag.
     */
    public boolean isAllowOverwrite() {
        return allowOverwrite;
    }

    /**
     * Sets a value indicating whether existing values can be overwritten by the data streamer.
     * <p>
     * Default is {@code false}.
     *
     * @param allowOverwrite Allow overwrite flag.
     * @return {@code this} for chaining.
     */
    public DataStreamerClientOptions<K, V> setAllowOverwrite(boolean allowOverwrite) {
        this.allowOverwrite = allowOverwrite;

        return this;
    }

    /**
     * Gets a flag indicating that write-through behavior should be disabled for data loading.
     * <p>
     * {@link #isAllowOverwrite()} must be {@code true} for write-through to work.
     * <p>
     * Default is {@code false}.
     *
     * @return Skip store flag.
     */
    public boolean isSkipStore() {
        return skipStore;
    }

    /**
     * Sets a flag indicating that write-through behavior should be disabled for data loading.
     * <p>
     * Default is {@code false}.
     *
     * @param skipStore Skip store flag.
     * @return {@code this} for chaining.
     */
    public DataStreamerClientOptions<K, V> setSkipStore(boolean skipStore) {
        this.skipStore = skipStore;

        return this;
    }

    /**
     * Gets a value indicating whether {@link #getReceiver()} should operate in binary mode.
     *
     * @return Receiver keep binary flag.
     */
    public boolean isReceiverKeepBinary() {
        return receiverKeepBinary;
    }

    /**
     * Sets a value indicating whether {@link #getReceiver()} should operate in binary mode.
     *
     * @param receiverKeepBinary Receiver keep binary flag.
     * @return {@code this} for chaining.
     */
    public DataStreamerClientOptions<K, V> setReceiverKeepBinary(boolean receiverKeepBinary) {
        this.receiverKeepBinary = receiverKeepBinary;

        return this;
    }

    /**
     * Gets a custom stream receiver.
     * Stream receiver is invoked for every cache entry on the primary server node for that entry.
     *
     * @return Stream receiver, or {@code null} if not set.
     */
    public StreamReceiver<K, V> getReceiver() {
        return receiver;
    }

    /**
     * Sets a custom stream receiver.
     * Stream receiver is invoked for every cache entry on the primary server node for that entry.
     *
     * @param receiver Stream receiver.
     * @return {@code this} for chaining.
     */
    public DataStreamerClientOptions<K, V> setReceiver(StreamReceiver<K, V> receiver) {
        this.receiver = receiver;

        return this;
    }
}

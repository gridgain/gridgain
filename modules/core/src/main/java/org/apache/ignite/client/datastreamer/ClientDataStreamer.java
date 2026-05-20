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

import org.apache.ignite.client.IgniteClientFuture;

/**
 * Thin client data streamer.
 * <p>
 * Data streamer is an efficient and fault-tolerant way to load data into cache. Updates are buffered and mapped
 * to primary nodes to ensure minimal data movement and optimal resource utilization.
 * Update failures caused by cluster topology changes are retried automatically.
 * <p>
 * Note that the streamer sends data to remote nodes asynchronously, so cache updates can be reordered.
 * <p>
 * Instances of the implementing class are thread-safe: data can be added from multiple threads.
 * <p>
 * Closing: {@link AutoCloseable#close()} calls {@link #close(boolean)} with {@code false}.
 * This will flush any remaining data to the cache synchronously.
 * To avoid blocking when closing, use {@link #closeAsync(boolean)}.
 */
public interface ClientDataStreamer<K, V> extends AutoCloseable {
    /**
     * Gets the cache name.
     *
     * @return Cache name.
     */
    public String cacheName();

    /**
     * Gets a value indicating whether this streamer is closed.
     *
     * @return {@code true} if closed, {@code false} otherwise.
     */
    public boolean isClosed();

    /**
     * Gets the options.
     *
     * @return Data streamer options.
     */
    public DataStreamerClientOptions<K, V> options();

    /**
     * Adds an entry to the streamer.
     * <p>
     * This method adds an entry to the buffer. When the buffer gets full, it is scheduled for
     * asynchronous background flush. This method will block when the number of active flush operations
     * exceeds {@link DataStreamerClientOptions#getPerNodeParallelOperations()}.
     *
     * @param key Key.
     * @param val Value. When {@code null}, the cache entry will be removed.
     */
    public void add(K key, V val);

    /**
     * Adds a removal entry to the streamer. The cache entry with the specified key will be removed.
     * <p>
     * Removal requires {@link DataStreamerClientOptions#isAllowOverwrite()} to be {@code true}.
     * <p>
     * This method adds an entry to the buffer. When the buffer gets full, it is scheduled for
     * asynchronous background flush. This method will block when the number of active flush operations
     * exceeds {@link DataStreamerClientOptions#getPerNodeParallelOperations()}.
     *
     * @param key Key to remove.
     */
    public void remove(K key);

    /**
     * Flushes all buffered entries synchronously.
     */
    public void flush();

    /**
     * Flushes all buffered entries asynchronously.
     *
     * @return Future representing pending completion of the flush.
     */
    public IgniteClientFuture<Void> flushAsync();

    /**
     * Closes this streamer, optionally cancelling any remaining data load into the cache.
     *
     * @param cancel When {@code true}, there is no guarantee which part of remaining data will be loaded
     *               into the cache.
     */
    public void close(boolean cancel);

    /** {@inheritDoc} */
    @Override public default void close() {
        close(false);
    }

    /**
     * Closes this streamer asynchronously, optionally cancelling any remaining data load into the cache.
     *
     * @param cancel When {@code true}, there is no guarantee which part of remaining data will be loaded
     *               into the cache.
     * @return Future representing pending completion of the close.
     */
    public IgniteClientFuture<Void> closeAsync(boolean cancel);
}

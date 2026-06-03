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

import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.stream.StreamReceiver;

import java.util.Collection;
import java.util.Map;

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
 */
public interface ClientDataStreamer<K, V> extends AutoCloseable {
    /** Default limit for parallel operations per server node. Calculated as available processors times 4. */
    public static final int DFLT_PER_NODE_PARALLEL_OPERATIONS = Runtime.getRuntime().availableProcessors() * 4;

    /**
     * Gets the cache name.
     *
     * @return Cache name.
     */
    public String cacheName();

    /**
     * Gets flag enabling overwriting existing values in cache.
     * Data streamer will perform better if this flag is disabled.
     * <p>
     * This flag is disabled by default (default is {@code false}).
     *
     * @return {@code True} if overwriting is allowed, {@code false} otherwise.
     */
    public boolean allowOverwrite();

    /**
     * Sets flag enabling overwriting existing values in cache.
     * Data streamer will perform better if this flag is disabled.
     * Note that when this flag is {@code false}, updates will not be propagated to the cache store
     * (i.e. {@link #skipStore()} flag will be set to {@code true} implicitly).
     * <p>
     * This flag is disabled by default (default is {@code false}).
     * <p>
     * The flag has no effect when custom cache receiver set using {@link #receiver(StreamReceiver)} method.
     *
     * @param allowOverwrite Flag value.
     */
    public void allowOverwrite(boolean allowOverwrite);

    /**
     * Gets flag indicating that write-through behavior should be disabled for data streaming.
     * Default is {@code false}.
     *
     * @return Skip store flag.
     */
    public boolean skipStore();

    /**
     * Sets flag indicating that write-through behavior should be disabled for data streaming.
     * Default is {@code false}.
     *
     * @param skipStore Skip store flag.
     */
    public void skipStore(boolean skipStore);

    /**
     * Gets flag indicating that objects should be kept in binary format when passed to the stream receiver.
     * Default is {@code false}.
     *
     * @return Keep binary flag.
     */
    public boolean keepBinary();

    /**
     * Sets flag indicating that objects should be kept in binary format when passed to the stream receiver.
     * Default is {@code false}.
     *
     * @param keepBinary Keep binary flag.
     */
    public void keepBinary(boolean keepBinary);

    /**
     * Gets size of per node key-value pairs buffer.
     *
     * @return Per node buffer size.
     */
    public int perNodeBufferSize();

    /**
     * Sets size of per node key-value pairs buffer.
     * <p>
     * This method should be called prior to {@link #addData(Object, Object)} call.
     * <p>
     * If not provided, default value is {@link org.apache.ignite.IgniteDataStreamer#DFLT_PER_NODE_BUFFER_SIZE}.
     *
     * @param bufSize Per node buffer size.
     */
    public void perNodeBufferSize(int bufSize);

    /**
     * Gets maximum number of parallel stream operations for a single node.
     *
     * @return Maximum number of parallel stream operations for a single node.
     */
    public int perNodeParallelOperations();

    /**
     * Sets maximum number of parallel stream operations for a single node.
     * <p>
     * This method should be called prior to {@link #addData(Object, Object)} call.
     * <p>
     * If not provided, default value is {@link #DFLT_PER_NODE_PARALLEL_OPERATIONS}.
     *
     * @param parallelOps Maximum number of parallel stream operations for a single node.
     * @see IgniteConfiguration#getDataStreamerThreadPoolSize()
     */
    public void perNodeParallelOperations(int parallelOps);

    /**
     * Sets the timeout that is used in the following cases:
     * <ul>
     * <li>any data addition method can be blocked when all per node parallel operations are exhausted.
     * The timeout defines the max time you will be blocked waiting for a permit to add a chunk of data
     * into the streamer;</li>
     * <li>Total timeout time for {@link #flush()} operation;</li>
     * <li>Total timeout time for {@link #close()} operation.</li>
     * </ul>
     * By default the timeout is disabled.
     *
     * @param timeout Timeout in milliseconds.
     * @throws IllegalArgumentException If timeout is zero or less than {@code -1}.
     */
    public void timeout(long timeout);

    /**
     * Gets timeout set by {@link #timeout(long)}.
     *
     * @return Timeout in milliseconds.
     */
    public long timeout();

    /**
     * Gets automatic flush frequency. Essentially, this is the time after which the
     * streamer will make an attempt to submit all data added so far to remote nodes.
     * Note that there is no guarantee that data will be delivered after this concrete
     * attempt (e.g., it can fail when topology is changing), but it won't be lost anyway.
     * <p>
     * If set to {@code 0}, automatic flush is disabled.
     * <p>
     * Automatic flush is disabled by default (default value is {@code 0}).
     *
     * @return Flush frequency or {@code 0} if automatic flush is disabled.
     * @see #flush()
     */
    public long autoFlushFrequency();

    /**
     * Sets automatic flush frequency. Essentially, this is the time after which the
     * streamer will make an attempt to submit all data added so far to remote nodes.
     * Note that there is no guarantee that data will be delivered after this concrete
     * attempt (e.g., it can fail when topology is changing), but it won't be lost anyway.
     * <p>
     * If set to {@code 0}, automatic flush is disabled.
     * <p>
     * Automatic flush is disabled by default (default value is {@code 0}).
     *
     * @param autoFlushFreq Flush frequency or {@code 0} to disable automatic flush.
     * @see #flush()
     */
    public void autoFlushFrequency(long autoFlushFreq);

    /**
     * Gets future for this streaming process. This future completes whenever method
     * {@link #close(boolean)} completes. By attaching listeners to this future
     * it is possible to get asynchronous notifications for completion of this
     * streaming process.
     *
     * @return Future for this streaming process.
     */
    public IgniteClientFuture<Void> future();

    /**
     * Sets custom stream receiver to this data streamer.
     *
     * @param rcvr Stream receiver.
     */
    public void receiver(StreamReceiver<K, V> rcvr);

    /**
     * Adds key for removal on remote node. Equivalent to {@link #addData(Object, Object) addData(key, null)}.
     *
     * @param key Key.
     * @return Future for this operation.
     *      Note: It may never complete unless {@link #flush()} or {@link #close()} are explicitly called.
     * @throws ClientException If failed to map key to node.
     */
    public IgniteClientFuture<Void> removeData(K key) throws ClientException, InterruptedException;

    /**
     * Adds data for streaming on remote node. This method can be called from multiple
     * threads in parallel to speed up streaming if needed.
     * <p>
     * Note that streamer will stream data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the streamer. The data may not be sent until {@link #flush()} or {@link #close()} are called.
     * <p>
     * Note: if {@link #allowOverwrite()} set to {@code false} (by default)
     * then data streamer will not overwrite existing cache entries for better performance
     * (to change, set {@link #allowOverwrite(boolean)} to {@code true})
     *
     * @param key Key.
     * @param val Value or {@code null} if respective entry must be removed from cache.
     * @return Future for this operation.
     *      Note: It may never complete unless {@link #flush()} or {@link #close()} are explicitly called.
     * @throws ClientException If failed to map key to node.
     * @see #allowOverwrite()
     */
    public IgniteClientFuture<Void> addData(K key, V val) throws ClientException, InterruptedException;

    /**
     * Adds data for streaming on remote node. This method can be called from multiple
     * threads in parallel to speed up streaming if needed.
     * <p>
     * Note that streamer will stream data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the streamer. The data may not be sent until {@link #flush()} or {@link #close()} are called.
     * <p>
     * Note: if {@link #allowOverwrite()} set to {@code false} (by default)
     * then data streamer will not overwrite existing cache entries for better performance
     * (to change, set {@link #allowOverwrite(boolean)} to {@code true})
     *
     * @param entry Entry.
     * @return Future for this operation.
     *      Note: It may never complete unless {@link #flush()} or {@link #close()} are explicitly called.
     * @throws ClientException If failed to map key to node.
     * @see #allowOverwrite()
     */
    public IgniteClientFuture<Void> addData(Map.Entry<K, V> entry) throws ClientException, InterruptedException;

    /**
     * Adds data for streaming on remote node. This method can be called from multiple
     * threads in parallel to speed up streaming if needed.
     * <p>
     * Note that streamer will stream data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the streamer. The data may not be sent until {@link #flush()} or {@link #close()} are called.
     * <p>
     * Note: if {@link #allowOverwrite()} set to {@code false} (by default)
     * then data streamer will not overwrite existing cache entries for better performance
     * (to change, set {@link #allowOverwrite(boolean)} to {@code true})
     *
     * @param entries Collection of entries to be streamed.
     * @return Future for this stream operation.
     *      Note: It may never complete unless {@link #flush()} or {@link #close()} are explicitly called.
     * @throws ClientException If failed to stream data.
     * @see #allowOverwrite()
     */
    public IgniteClientFuture<Void> addData(Collection<? extends Map.Entry<K, V>> entries)
            throws ClientException, InterruptedException;

    /**
     * Adds data for streaming on remote node. This method can be called from multiple
     * threads in parallel to speed up streaming if needed.
     * <p>
     * Note that streamer will stream data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the streamer. The data may not be sent until {@link #flush()} or {@link #close()} are called.
     * <p>
     * Note: if {@link #allowOverwrite()} set to {@code false} (by default)
     * then data streamer will not overwrite existing cache entries for better performance
     * (to change, set {@link #allowOverwrite(boolean)} to {@code true})
     *
     * @param entries Map to be streamed.
     * @return Future for this stream operation.
     *      Note: It may never complete unless {@link #flush()} or {@link #close()} are explicitly called.
     * @throws ClientException If failed to stream data.
     * @see #allowOverwrite()
     */
    public IgniteClientFuture<Void> addData(Map<K, V> entries) throws ClientException, InterruptedException;

    /**
     * Streams any remaining data, but doesn't close the streamer. Data can be still added after
     * flush is finished. This method blocks and doesn't allow to add any data until all data
     * is streamed.
     * <p>
     * If another thread is already performing flush, this method will block, wait for
     * another thread to complete flush and exit. If you don't want to wait in this case,
     * use {@link #tryFlush()} method.
     * <p>
     * Note that {@link #flush()} guarantees completion of all futures returned by {@link #addData(Object, Object)}, listeners
     * should be tracked separately.
     *
     * @throws ClientException If failed to flush data from buffer.
     * @see #tryFlush()
     */
    public void flush() throws ClientException;

    /**
     * Makes an attempt to stream remaining data. This method is mostly similar to {@link #flush},
     * with the difference that it won't wait and will exit immediately.
     *
     * @throws ClientException If failed to flush data from buffer.
     * @see #flush()
     */
    public void tryFlush() throws ClientException;

    /**
     * Streams any remaining data and closes this streamer.
     *
     * @param cancel {@code True} to cancel ongoing streaming operations.
     * @throws ClientException If failed to close data streamer.
     */
    public void close(boolean cancel) throws ClientException;

    /**
     * Closes data streamer. This method is identical to calling {@link #close(boolean) close(false)} method.
     * <p>
     * The method is invoked automatically on objects managed by the
     * {@code try-with-resources} statement.
     *
     * @throws ClientException If failed to close data streamer.
     */
    @Override public void close() throws ClientException;
}

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

namespace Apache.Ignite.Core.Datastream
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache.Store;

    /// <summary>
    /// Data streamer loads data efficiently into cache. Updates are buffered and mapped to primary nodes
    /// to ensure minimal data movement and optimal resource utilization.
    /// <para />
    /// Note that streamer loads data to remote nodes in parallel, so cache updates can be reordered.
    /// <para />
    /// Also note that <c>IDataStreamer</c> is not the only way to load data into cache.
    /// Alternatively you can use
    /// <see cref="ICacheStore{K, V}.LoadCache(Action{K, V}, object[])"/>
    /// method to load data from underlying data store. You can also use standard cache
    /// <c>put</c> and <c>putAll</c> operations as well, but they most likely will not perform
    /// as well as this class for loading data. And finally, data can be loaded from underlying
    /// data store on demand, whenever it is accessed - for this no explicit data loading step
    /// is needed.
    /// <para />
    /// <c>IDataStreamer</c> supports the following configuration properties:
    /// <list type="bullet">
    ///     <item>
    ///         <term>PerNodeBufferSize</term>
    ///         <description>When entries are added to data streamer they are not sent to Ignite
    ///         right away and are buffered internally for better performance and network utilization.
    ///         This setting controls the size of internal per-node buffer before buffered data is sent to
    ///         remote node. Default value is 1024.</description>
    ///     </item>
    ///     <item>
    ///         <term>PerThreadBufferSize</term>
    ///         <description>When entries are added to data streamer they are not sent to Ignite
    ///         right away and are buffered internally on per thread basis for better performance and network utilization.
    ///         This setting controls the size of internal per-thread buffer before buffered data is sent to
    ///         remote node. Default value is 4096.</description>
    ///     </item>
    ///     <item>
    ///         <term>PerNodeParallelOperations</term>
    ///         <description>Sometimes data may be added to the data streamer faster than it can be put
    ///         in cache. In this case, new buffered load messages are sent to remote nodes before
    ///         responses from previous ones are received. This could cause unlimited heap memory
    ///         utilization growth on local and remote nodes. To control memory utilization, this
    ///         setting limits maximum allowed number of parallel buffered load messages that are
    ///         being processed on remote nodes. If this number is exceeded, then data streamer add/remove
    ///         methods will block to control memory utilization. Default value is 16.</description>
    ///     </item>
    ///     <item>
    ///         <term>AutoFlushFrequency</term>
    ///         <description>Automatic flush frequency in milliseconds. Essentially, this is the time
    ///         after which the streamer will make an attempt to submit all data added so far to remote
    ///         nodes. Note that there is no guarantee that data will be delivered after this concrete
    ///         attempt (e.g., it can fail when topology is changing), but it won't be lost anyway.
    ///         Disabled by default (default value is <c>0</c>).</description>
    ///     </item>
    ///     <item>
    ///         <term>Isolated</term>
    ///         <description>Defines if data streamer will assume that there are no other concurrent
    ///         updates and allow data streamer choose most optimal concurrent implementation. Default value
    ///         is <c>false</c>.</description>
    ///     </item>
    /// </list>
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface IDataStreamer<TK, TV> : IDisposable
    {
        /// <summary>
        /// Name of the cache to load data to.
        /// </summary>
        string CacheName { get; }

        /// <summary>
        /// Gets or sets a value indicating whether existing values can be overwritten by the data streamer.
        /// Performance is better when this flag is false.
        /// <para />
        /// NOTE: When false, cache updates won't be propagated to cache store
        /// (even if <see cref="SkipStore"/> is false).
        /// <para />
        /// Default is <c>false</c>.
        /// </summary>
        bool AllowOverwrite { get; set; }

        /// <summary>
        /// Flag indicating that write-through behavior should be disabled for data loading.
        /// <para />
        /// <see cref="AllowOverwrite"/> must be true for write-through to work.
        /// <para />
        /// Default is <c>false</c>.
        /// </summary>
        bool SkipStore { get; set; }

        /// <summary>
        /// Size of per node key-value pairs buffer.
        /// <para />
        /// Setter must be called before any add/remove operation.
        /// <para />
        /// Default is <see cref="DataStreamerDefaults.DefaultPerNodeBufferSize"/>.
        /// </summary>
        [DefaultValue(DataStreamerDefaults.DefaultPerNodeBufferSize)]
        int PerNodeBufferSize { get; set; }

        /// <summary>
        /// Size of per thread key-value pairs buffer.
        /// <para />
        /// Setter must be called before any add/remove operation.
        /// <para />
        /// Default is <see cref="DataStreamerDefaults.DefaultPerThreadBufferSize"/>.
        /// </summary>
        [DefaultValue(DataStreamerDefaults.DefaultPerThreadBufferSize)]
        int PerThreadBufferSize { get; set; }

        /// <summary>
        /// Maximum number of parallel load operations for a single node.
        /// <para />
        /// Setter must be called before any add/remove operation.
        /// <para />
        /// Default is 0, which means Ignite calculates this automatically as
        /// <see cref="IgniteConfiguration.DataStreamerThreadPoolSize"/> *
        /// <see cref="DataStreamerDefaults.DefaultParallelOperationsMultiplier"/>.
        /// </summary>
        int PerNodeParallelOperations { get; set; }

        /// <summary>
        /// Gets or sets the automatic flush frequency, in milliseconds.
        /// Data streamer buffers the data for performance reasons.
        /// The buffer is flushed in the following cases:
        /// <ul>
        /// <li>Buffer is full.</li>
        /// <li><see cref="Flush"/> or <see cref="TryFlush"/> is called.</li>
        /// <li>Periodically when <see cref="AutoFlushInterval"/> is set.</li >
        /// </ul>
        /// <para />
        /// If set to <c>0</c>, automatic flush is disabled.
        /// <para />
        /// Default is <c>0</c> (disabled).
        /// </summary>
        [Obsolete("Use AutoFlushInterval.")]
        long AutoFlushFrequency { get; set; }

        /// <summary>
        /// Gets or sets the automatic flush interval. Data streamer buffers the data for performance reasons.
        /// The buffer is flushed in the following cases:
        /// <ul>
        /// <li>Buffer is full.</li>
        /// <li><see cref="Flush"/> or <see cref="TryFlush"/> is called.</li>
        /// <li>Periodically when <see cref="AutoFlushInterval"/> is set.</li >
        /// </ul>
        /// <para />
        /// When set to <see cref="TimeSpan.Zero"/>, automatic flush is disabled.
        /// <para />
        /// Default is <see cref="TimeSpan.Zero"/> (disabled).
        /// </summary>
        TimeSpan AutoFlushInterval { get; set; }

        /// <summary>
        /// Gets the task for this loading process. This task completes whenever method
        /// <see cref="IDataStreamer{K,V}.Close(bool)"/> completes.
        /// </summary>
        Task Task { get; }

        /// <summary>
        /// Gets or sets custom stream receiver.
        /// </summary>
        IStreamReceiver<TK, TV> Receiver { get; set; }

        /// <summary>
        /// Adds single key-value pair for loading. Passing <c>null</c> as value will be
        /// interpreted as removal.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        /// <returns>Task for this operation.</returns>
        [Obsolete("Use Add.")]
        Task AddData(TK key, TV val);

        /// <summary>
        /// Adds single key-value pair for loading. Passing <c>null</c> as pair's value will
        /// be interpreted as removal.
        /// </summary>
        /// <param name="pair">Key-value pair.</param>
        /// <returns>Task for this operation.</returns>
        [Obsolete("Use Add.")]
        Task AddData(KeyValuePair<TK, TV> pair);

        /// <summary>
        /// Adds collection of key-value pairs for loading.
        /// </summary>
        /// <param name="entries">Entries.</param>
        /// <returns>Task for this operation.</returns>
        [Obsolete("Use Add.")]
        Task AddData(ICollection<KeyValuePair<TK, TV>> entries);

        /// <summary>
        /// Adds key for removal.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <returns>Task for this operation.</returns>
        [Obsolete("Use Remove.")]
        Task RemoveData(TK key);

        /// <summary>
        /// Adds single key-value pair for loading. Passing <c>null</c> as value will be
        /// interpreted as removal.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        void Add(TK key, TV val);

        /// <summary>
        /// Adds single key-value pair for loading. Passing <c>null</c> as pair's value will
        /// be interpreted as removal.
        /// </summary>
        /// <param name="pair">Key-value pair.</param>
        void Add(KeyValuePair<TK, TV> pair);

        /// <summary>
        /// Adds collection of key-value pairs for loading.
        /// </summary>
        /// <param name="entries">Entries.</param>
        void Add(ICollection<KeyValuePair<TK, TV>> entries);

        /// <summary>
        /// Adds key for removal.
        /// </summary>
        /// <param name="key">Key.</param>
        void Remove(TK key);

        /// <summary>
        /// Makes an attempt to load remaining data. This method is mostly similar to
        /// <see cref="IDataStreamer{K,V}.Flush()"/> with the difference that it won't wait and
        /// will exit immediately.
        /// </summary>
        [Obsolete("Use FlushAsync")]
        void TryFlush();

        /// <summary>
        /// Loads any remaining buffered data, but doesn't close the streamer.
        /// </summary>
        void Flush();

        /// <summary>
        /// Loads any remaining buffered data, but doesn't close the streamer.
        /// </summary>
        Task FlushAsync();

        /// <summary>
        /// Closes this streamer, optionally loading any remaining data into the cache.
        /// </summary>
        /// <param name="cancel">Whether to cancel ongoing loading operations. When set to <c>true</c>,
        /// there is no guarantee which part of remaining data will be actually loaded into the cache.</param>
        void Close(bool cancel);

        /// <summary>
        /// Gets streamer instance with binary mode enabled, changing key and/or value types if necessary.
        /// In binary mode stream receiver gets data in binary format.
        /// You can only change key/value types when transitioning from non-binary to binary streamer;
        /// Changing type of binary streamer is not allowed and will throw an <see cref="InvalidOperationException"/>
        /// </summary>
        /// <typeparam name="TK1">Key type in binary mode.</typeparam>
        /// <typeparam name="TV1">Value type in binary mode.</typeparam>
        /// <returns>Streamer instance with binary mode enabled.</returns>
        IDataStreamer<TK1, TV1> WithKeepBinary<TK1, TV1>();

        /// <summary>
        /// Gets or sets the timeout. Negative values mean no timeout.
        /// Default is <see cref="DataStreamerDefaults.DefaultTimeout"/>.
        /// <para />
        /// Timeout is used in the following cases:
        /// <li>Any data addition method can be blocked when all per node parallel operations are exhausted.
        /// The timeout defines the max time you will be blocked waiting for a permit to add a chunk of data
        /// into the streamer;</li>
        /// <li>Total timeout time for <see cref="Flush"/> operation;</li>
        /// <li>Total timeout time for <see cref="Close"/> operation.</li>
        /// </summary>
        TimeSpan Timeout { get; set; }

        /// <summary>
        /// Gets the task for the current batch. This task completes when current and all previous batches are flushed,
        /// either explicitly with <see cref="Flush"/>, or automatically when the buffer is full or
        /// <see cref="AutoFlushFrequency"/> is set.
        /// </summary>
        Task GetCurrentBatchTask();
    }
}

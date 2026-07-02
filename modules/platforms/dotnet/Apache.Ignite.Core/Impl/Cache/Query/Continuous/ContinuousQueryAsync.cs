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

#if NET8_0_OR_GREATER
namespace Apache.Ignite.Core.Impl.Cache.Query.Continuous
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Channels;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query.Continuous;

    /// <summary>
    /// Bridges the callback-based continuous query API to <see cref="IAsyncEnumerable{T}"/>.
    /// <para />
    /// Cache update events are pushed into an unbounded channel by <see cref="ChannelListener{TK,TV}"/>
    /// and pulled out by the async stream. This is the shared machinery behind the
    /// <c>QueryContinuousAsync</c> default interface methods on <see cref="ICache{TK,TV}"/>.
    /// </summary>
    internal static class ContinuousQueryAsync
    {
        /// <summary>
        /// Creates the channel used to hand events off from the listener callback to the async stream.
        /// </summary>
        public static Channel<ICacheEntryEvent<TK, TV>> CreateChannel<TK, TV>()
        {
            // Unbounded: the listener callback must never block the Ignite notification thread.
            // Single reader (one GetEvents/enumeration), multiple writers (batches may be delivered
            // from different threads over the query lifetime).
            return Channel.CreateUnbounded<ICacheEntryEvent<TK, TV>>(
                new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
        }

        /// <summary>
        /// Builds a <see cref="ContinuousQuery{TK,TV}"/> whose listener feeds the specified channel writer,
        /// applying the given options and optional filter.
        /// </summary>
        public static ContinuousQuery<TK, TV> CreateQuery<TK, TV>(
            ChannelWriter<ICacheEntryEvent<TK, TV>> writer,
            ContinuousQueryOptions? options,
            ICacheEntryFilter<TK, TV>? filter)
            where TK : notnull
        {
            var eventFilter = filter == null ? null : new EntryEventFilter<TK, TV>(filter);

            var qry = new ContinuousQuery<TK, TV>(new ChannelListener<TK, TV>(writer), eventFilter, loc: false);

            if (options != null)
            {
                qry.BufferSize = options.BufferSize;
                qry.TimeInterval = options.TimeInterval;
                qry.AutoUnsubscribe = options.AutoUnsubscribe;
                qry.Local = options.Local;
                qry.IncludeExpired = options.IncludeExpired;
            }

            return qry;
        }

        /// <summary>
        /// Reads events from the channel until it is completed (query stopped / handle disposed) or the
        /// enumeration is cancelled. The <see cref="EnumeratorCancellationAttribute"/> makes cancellation
        /// flow through <c>await foreach (... .WithCancellation(token))</c>.
        /// </summary>
        public static async IAsyncEnumerable<ICacheEntryEvent<TK, TV>> ReadEvents<TK, TV>(
            ChannelReader<ICacheEntryEvent<TK, TV>> reader,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (reader.TryRead(out var evt))
                {
                    yield return evt;
                }
            }
        }

        /// <summary>
        /// Continuous query listener that forwards events into a channel.
        /// </summary>
        private sealed class ChannelListener<TK, TV> : ICacheEntryEventListener<TK, TV>
        {
            private readonly ChannelWriter<ICacheEntryEvent<TK, TV>> _writer;

            public ChannelListener(ChannelWriter<ICacheEntryEvent<TK, TV>> writer)
            {
                _writer = writer;
            }

            /** <inheritdoc /> */
            public void OnEvent(IEnumerable<ICacheEntryEvent<TK, TV>> evts)
            {
                foreach (var evt in evts)
                {
                    // Unbounded channel: TryWrite only fails once the writer is completed (handle disposed),
                    // in which case dropping late events is the desired behavior.
                    _writer.TryWrite(evt);
                }
            }
        }

        /// <summary>
        /// Adapts a plain <see cref="ICacheEntryFilter{TK,TV}"/> to the continuous-query
        /// <see cref="ICacheEntryEventFilter{TK,TV}"/> (an event is a cache entry).
        /// </summary>
        [Serializable]
        private sealed class EntryEventFilter<TK, TV> : ICacheEntryEventFilter<TK, TV>
        {
            private readonly ICacheEntryFilter<TK, TV> _filter;

            public EntryEventFilter(ICacheEntryFilter<TK, TV> filter)
            {
                _filter = filter;
            }

            /** <inheritdoc /> */
            public bool Evaluate(ICacheEntryEvent<TK, TV> evt)
            {
                return _filter.Invoke(evt);
            }
        }
    }
}
#endif

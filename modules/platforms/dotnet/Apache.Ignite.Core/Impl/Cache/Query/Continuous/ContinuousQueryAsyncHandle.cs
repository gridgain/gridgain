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
    using System.Threading;
    using System.Threading.Channels;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query.Continuous;

    /// <summary>
    /// Asynchronous continuous query handle that wraps a synchronous
    /// <see cref="IContinuousQueryHandle"/> executed with an initial query, exposing the initial query
    /// cursor and the event stream. Disposing stops the underlying query and completes the event stream.
    /// </summary>
    /// <typeparam name="TK">Key type.</typeparam>
    /// <typeparam name="TV">Value type.</typeparam>
    /// <typeparam name="TInitial">Initial query cursor type.</typeparam>
    internal sealed class ContinuousQueryAsyncHandle<TK, TV, TInitial>
        : IContinuousQueryHandleAsync<TK, TV, TInitial>
    {
        private readonly IDisposable _handle;
        private readonly Channel<ICacheEntryEvent<TK, TV>> _channel;
        private readonly Func<TInitial> _initialCursor;

        private int _eventsRequested;

        public ContinuousQueryAsyncHandle(
            IDisposable handle,
            Channel<ICacheEntryEvent<TK, TV>> channel,
            Func<TInitial> initialCursor)
        {
            _handle = handle;
            _channel = channel;
            _initialCursor = initialCursor;
        }

        /** <inheritdoc /> */
        public IAsyncEnumerable<ICacheEntryEvent<TK, TV>> GetEvents()
        {
            if (Interlocked.CompareExchange(ref _eventsRequested, 1, 0) != 0)
            {
                throw new InvalidOperationException("GetEvents can be called only once.");
            }

            return ContinuousQueryAsync.ReadEvents(_channel.Reader);
        }

        /** <inheritdoc /> */
        public TInitial GetInitialQueryCursor()
        {
            // Delegates to the underlying handle, which enforces the "only once" semantics.
            return _initialCursor();
        }

        /** <inheritdoc /> */
        public ValueTask DisposeAsync()
        {
            _handle.Dispose();

            // Unblock a pending GetEvents() enumeration: the reader drains buffered events, then stops.
            _channel.Writer.TryComplete();

            return default;
        }
    }
}
#endif

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

namespace Apache.Ignite.Core.Impl.Cache.Query.Continuous;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Apache.Ignite.Core.Cache.Event;
using Apache.Ignite.Core.Cache.Query.Continuous;

/// <summary>
/// Asynchronous continuous query handle that wraps a synchronous
/// <see cref="IContinuousQueryHandle"/> executed with an initial query, exposing the initial query
/// cursor and the event stream. The query is stopped and server-side resources are released when the
/// handle is disposed or when event enumeration ends (via break, cancellation, or completion), whichever
/// happens first.
/// </summary>
/// <typeparam name="TK">Key type.</typeparam>
/// <typeparam name="TV">Value type.</typeparam>
/// <typeparam name="TInitial">Initial query cursor type.</typeparam>
internal sealed class ContinuousQueryAsyncHandle<TK, TV, TInitial>(
    IDisposable handle,
    Channel<ICacheEntryEvent<TK, TV>> channel,
    Func<TInitial> initialCursor)
    : IContinuousQueryHandleAsync<TK, TV, TInitial>
{
    private int _eventsRequested;
    private int _stopped;

    /** <inheritdoc /> */
    public IAsyncEnumerable<ICacheEntryEvent<TK, TV>> GetEvents()
    {
        if (Interlocked.CompareExchange(ref _eventsRequested, 1, 0) != 0)
        {
            throw new InvalidOperationException("GetEvents can be called only once.");
        }

        return EnumerateEvents();
    }

    /** <inheritdoc /> */
    public TInitial GetInitialQueryCursor() => initialCursor();

    /** <inheritdoc /> */
    public ValueTask DisposeAsync()
    {
        StopQuery();

        return default;
    }

    private async IAsyncEnumerable<ICacheEntryEvent<TK, TV>> EnumerateEvents(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        try
        {
            await foreach (var evt in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return evt;
            }
        }
        finally
        {
            StopQuery();
        }
    }

    /// <summary>
    /// Stops the underlying continuous query and completes the event channel. Idempotent, so it is safe to
    /// call from both <see cref="DisposeAsync"/> and the end of event enumeration.
    /// </summary>
    private void StopQuery()
    {
        if (Interlocked.CompareExchange(ref _stopped, 1, 0) != 0)
        {
            return;
        }

        handle.Dispose();

        // Unblock a pending GetEvents() enumeration: the reader drains buffered events, then stops.
        channel.Writer.TryComplete();
    }
}
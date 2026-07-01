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

namespace Apache.Ignite.Core.Cache.Query.Continuous;

using System;
using Apache.Ignite.Core.Cache.Event;

/// <summary>
/// Continuous query base options.
/// </summary>
public sealed record ContinuousQueryOptions
{
    /// <summary>
    /// Buffer size. When a cache update happens, entry is first put into a buffer.
    /// Entries from buffer will be sent to the master node only if the buffer is
    /// full or time provided via <see cref="TimeInterval"/> is exceeded.
    /// <para />
    /// Defaults to <see cref="ContinuousQuery.DefaultBufferSize"/>
    /// </summary>
    public int BufferSize { get; set; }

    /// <summary>
    /// Time interval. When a cache update happens, entry is first put into a buffer.
    /// Entries from buffer will be sent to the master node only if the buffer is full
    /// (its size can be provided via <see cref="BufferSize"/> property) or time provided
    /// via this method is exceeded.
    /// <para />
    /// Defaults to <c>0</c> which means that time check is disabled and entries will be
    /// sent only when buffer is full.
    /// </summary>
    public TimeSpan TimeInterval { get; set; }

    /// <summary>
    /// Automatic unsubscribe flag. This flag indicates that query filters on remote nodes
    /// should be automatically unregistered if master node (node that initiated the query)
    /// leaves topology. If this flag is <c>false</c>, filters will be unregistered only
    /// when the query is cancelled from master node, and won't ever be unregistered if
    /// master node leaves grid.
    /// <para />
    /// Defaults to <c>true</c>.
    /// </summary>
    public bool AutoUnsubscribe { get; set; }

    /// <summary>
    /// Local flag. When set query will be executed only on local node, so only local
    /// entries will be returned as query result.
    /// <para />
    /// Defaults to <c>false</c>.
    /// </summary>
    public bool Local { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether to notify about <see cref="CacheEntryEventType.Expired"/> events.
    /// <para />
    /// If <c>true</c>, then the remote listener will get notifications about expired cache entries.
    /// Otherwise, only <see cref="CacheEntryEventType.Created"/>, <see cref="CacheEntryEventType.Updated"/>, and
    /// <see cref="CacheEntryEventType.Removed"/> events will be passed to the listener.
    /// <para />
    /// Defaults to <c>false</c>.
    /// </summary>
    public bool IncludeExpired { get; set; }
}
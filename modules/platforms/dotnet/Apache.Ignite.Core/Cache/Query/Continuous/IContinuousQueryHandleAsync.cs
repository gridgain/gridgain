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

namespace Apache.Ignite.Core.Cache.Query.Continuous
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Cache.Event;

    /// <summary>
    /// Represents an asynchronous continuous query handle.
    /// <para />
    /// Disposing the handle stops the continuous query and releases the associated server-side resources.
    /// </summary>
    /// <typeparam name="TK">Key type.</typeparam>
    /// <typeparam name="TV">Value type.</typeparam>
    /// <typeparam name="TInitial">Initial query cursor type.</typeparam>
    public interface IContinuousQueryHandleAsync<out TK, out TV, out TInitial> : IAsyncDisposable
    {
        /// <summary>
        /// Gets the stream of cache entry events.
        /// <para />
        /// Enumeration continues until the handle is disposed or enumeration is stopped by the caller.
        /// Can be called only once, throws exception on consequent calls.
        /// </summary>
        /// <returns>Async stream of cache entry events.</returns>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate",
            Justification = "Semantics: result differs from call to call.")]
        IAsyncEnumerable<ICacheEntryEvent<TK, TV>> GetEvents();

        /// <summary>
        /// Gets the cursor for the initial query. The initial query is executed before the continuous
        /// listener is registered, which allows to iterate through entries that already existed at the
        /// time the continuous query was started.
        /// <para />
        /// Can be called only once, throws exception on consequent calls.
        /// </summary>
        /// <returns>Initial query cursor.</returns>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate",
            Justification = "Semantics: result differs from call to call.")]
        TInitial GetInitialQueryCursor();
    }
}

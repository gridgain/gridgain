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

namespace Apache.Ignite.Core.Impl.Cache.Query
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Query;

    /// <summary>
    /// Query cursor that wraps IEnumerable.
    /// </summary>
    public class EnumerableQueryCursor<T> : IQueryCursor<T>
    {
        /** */
        private readonly IEnumerable<T> _enumerable;

        /// <summary>
        /// Initializes a new instance of <see cref="EnumerableQueryCursor{T}"/>.
        /// </summary>
        /// <param name="enumerable">Enumerable to wrap.</param>
        public EnumerableQueryCursor(IEnumerable<T> enumerable)
        {
            Debug.Assert(enumerable != null);
            
            _enumerable = enumerable;
        }

        /** <inheritdoc /> */
        public IList<T> GetAll()
        {
            return _enumerable.ToList();
        }

        /** <inheritdoc /> */
        public IEnumerator<T> GetEnumerator()
        {
            return _enumerable.GetEnumerator();
        }

        /** <inheritdoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            var disposable = _enumerable as IDisposable;

            if (disposable != null)
            {
                disposable.Dispose();
            }
        }
    }
}
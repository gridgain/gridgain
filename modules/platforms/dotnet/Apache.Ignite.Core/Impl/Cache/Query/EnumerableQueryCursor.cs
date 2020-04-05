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
    public sealed class EnumerableQueryCursor<T> : IQueryCursor<T>
    {
        /** */
        private readonly IEnumerable<T> _enumerable;

        /** */
        private readonly Action _dispose;

        /** */
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of <see cref="EnumerableQueryCursor{T}"/>.
        /// </summary>
        /// <param name="enumerable">Enumerable to wrap.</param>
        /// <param name="dispose">Dispose action.</param>
        public EnumerableQueryCursor(IEnumerable<T> enumerable, Action dispose = null)
        {
            Debug.Assert(enumerable != null);
            
            _enumerable = enumerable;
            _dispose = dispose;

            if (_dispose == null)
            {
                GC.SuppressFinalize(this);
            }
        }

        /** <inheritdoc /> */
        public IList<T> GetAll()
        {
            ThrowIfDisposed();

            var res = _enumerable.ToList();
            
            Dispose();

            return res;
        }

        /** <inheritdoc /> */
        public IEnumerator<T> GetEnumerator()
        {
            ThrowIfDisposed();
            
            return _enumerable.GetEnumerator();
        }

        /** <inheritdoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            ThrowIfDisposed();
            
            return GetEnumerator();
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            ReleaseUnmanagedResources();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if this instance has been disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().Name, 
                    "Object has been disposed. Query cursor can not be enumerated multiple times");
            }
        }

        /// <summary>
        /// Releases unmanaged resources.
        /// </summary>
        private void ReleaseUnmanagedResources()
        {
            if (!_disposed)
            {
                _disposed = true;

                if (_dispose != null)
                {
                    _dispose();
                }
            }
        }

        /// <summary>
        /// Finalizer.
        /// </summary>
        ~EnumerableQueryCursor()
        {
            ReleaseUnmanagedResources();
        }
    }
}
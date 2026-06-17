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

#nullable disable

namespace Apache.Ignite.Core.Impl.Cache.Query
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Abstract query cursor implementation.
    /// </summary>
    internal abstract class QueryCursorBase<T> : IQueryCursor<T>, IEnumerator<T>, IAsyncEnumerator<T>
    {
        /** Position before head. */
        private const int BatchPosBeforeHead = -1;

        /** Keep binary flag. */
        private readonly bool _keepBinary;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Read func. */
        private readonly Func<BinaryReader, T> _readFunc;

        /** Lock object. */
        private readonly object _syncRoot = new object();

        /** Whether "GetAll" was called. */
        private bool _getAllCalled;

        /** Whether "GetEnumerator" was called. */
        private bool _iterCalled;

        /** Batch with entries. */
        private T[] _batch;

        /** Current position in batch. */
        private int _batchPos = BatchPosBeforeHead;

        /** Disposed flag. */
        private volatile bool _disposed;

        /** Whether next batch is available. */
        private bool _hasNext = true;

        /** Cancellation token captured by GetAsyncEnumerator and observed between async batch fetches. */
        private CancellationToken _asyncEnumeratorToken;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        /// <param name="readFunc">The read function.</param>
        /// <param name="initialBatchStream">Optional stream with initial batch.</param>
        protected QueryCursorBase(Marshaller marsh, bool keepBinary, Func<BinaryReader, T> readFunc,
            IBinaryStream initialBatchStream = null)
        {
            Debug.Assert(marsh != null);

            _keepBinary = keepBinary;
            _readFunc = readFunc;
            _marsh = marsh;

            if (initialBatchStream != null)
            {
                _batch = ConvertGetBatch(initialBatchStream);
            }
        }

        /** <inheritdoc /> */
        public IList<T> GetAll()
        {
            if (_getAllCalled)
                throw new InvalidOperationException("Failed to get all entries because GetAll() " +
                                                    "method has already been called.");

            if (_iterCalled)
                throw new InvalidOperationException("Failed to get all entries because GetEnumerator() " +
                                                    "method has already been called.");

            lock (_syncRoot)
            {
                ThrowIfDisposed();

                var res = GetAllInternal();

                _getAllCalled = true;
                _hasNext = false;

                return res;
            }
        }

        /** <inheritdoc /> */
        public async Task<IList<T>> GetAllAsync(CancellationToken cancellationToken = default)
        {
            if (_getAllCalled)
                throw new InvalidOperationException("Failed to get all entries because GetAll() " +
                                                    "method has already been called.");

            if (_iterCalled)
                throw new InvalidOperationException("Failed to get all entries because GetEnumerator() " +
                                                    "method has already been called.");

            ThrowIfDisposed();

            var res = await GetAllInternalAsync(cancellationToken).ConfigureAwait(false);

            _getAllCalled = true;
            _hasNext = false;

            return res;
        }

        #region Public IEnumerable methods

        /** <inheritdoc /> */
        public IEnumerator<T> GetEnumerator() => GetEnumeratorInternal();

        private QueryCursorBase<T> GetEnumeratorInternal()
        {
            if (_getAllCalled)
            {
                throw new InvalidOperationException("Failed to get enumerator entries because " +
                                                    "GetAll() method has already been called.");
            }

            if (_iterCalled)
            {
                throw new InvalidOperationException("Failed to get enumerator entries because " +
                                                    "GetEnumerator() method has already been called.");
            }

            ThrowIfDisposed();

            InitIterator();

            _iterCalled = true;

            return this;
        }

        protected abstract void InitIterator();

        /** <inheritdoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            // The cursor is its own single-use enumerator, so capture the token in a field and observe it
            // in MoveNextAsync. Cancellation is cooperative: an already-issued batch request (network or JVM
            // call) can not be aborted mid-flight, but cancellation is honored before each new batch is fetched.
            var enumerator = GetEnumeratorInternal();

            _asyncEnumeratorToken = cancellationToken;

            return enumerator;
        }

        #endregion

        #region Public IEnumerator methods

        /// <summary>
        /// Gets the element in the collection at the current position of the enumerator.
        /// </summary>
        public T Current
        {
            get
            {
                ThrowIfDisposed();

                lock (_syncRoot)
                {
                    if (_batchPos == BatchPosBeforeHead)
                        throw new InvalidOperationException("MoveNext has not been called.");

                    if (_batch == null)
                        throw new InvalidOperationException("Previous call to MoveNext returned false.");

                    return _batch[_batchPos];
                }
            }
        }

        /** <inheritdoc /> */
        object IEnumerator.Current
        {
            get { return Current; }
        }

        /** <inheritdoc /> */
        public bool MoveNext()
        {
            ThrowIfDisposed();

            lock (_syncRoot)
            {
                return MoveNextLocked();
            }
        }

        private bool MoveNextLocked()
        {
            Debug.Assert(Monitor.IsEntered(_syncRoot), "_syncRoot must be held");

            ThrowIfDisposed();

            if (_batch == null)
            {
                if (_batchPos == BatchPosBeforeHead)
                    // Standing before head, let's get batch and advance position.
                    RequestBatch();
            }
            else
            {
                _batchPos++;

                if (_batch.Length == _batchPos)
                    // Reached batch end => request another.
                    RequestBatch();
            }

            return _batch != null;
        }

        protected async ValueTask<IList<T>> EnumerateAllAsync(CancellationToken cancellationToken)
        {
            var res = new List<T>();

            while (await MoveNextCoreAsync(cancellationToken).ConfigureAwait(false))
            {
                res.Add(_batch[_batchPos]);
            }

            return res;
        }

        public async ValueTask<bool> MoveNextAsync()
        {
            // Not locked: an async enumerator is consumed sequentially (await foreach), so there is no
            // concurrent MoveNextAsync. Use-after-dispose is observed via the volatile _disposed flag.
            ThrowIfDisposed();

            return await MoveNextCoreAsync(_asyncEnumeratorToken).ConfigureAwait(false);
        }

        private async ValueTask<bool> MoveNextCoreAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            if (_batch == null)
            {
                if (_batchPos == BatchPosBeforeHead)
                    // Standing before head, let's get batch and advance position.
                    await RequestBatchAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                _batchPos++;

                if (_batch.Length == _batchPos)
                    // Reached batch end => request another.
                    await RequestBatchAsync(cancellationToken).ConfigureAwait(false);
            }

            return _batch != null;
        }

        /** <inheritdoc /> */
        public void Reset()
        {
            throw new NotSupportedException("Reset is not supported.");
        }

        #endregion

        /// <summary>
        /// Gets all entries.
        /// </summary>
        protected abstract IList<T> GetAllInternal();

        /// <summary>
        /// Gets all entries asynchronously.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token observed before each round-trip to the server.</param>
        protected abstract ValueTask<IList<T>> GetAllInternalAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Requests next batch.
        /// </summary>
        private void RequestBatch()
        {
            _batch = _hasNext
                ? GetBatch()
                : null;

            _batchPos = 0;
        }

        /// <summary>
        /// Requests next batch.
        /// </summary>
        private async ValueTask RequestBatchAsync(CancellationToken cancellationToken)
        {
            _batch = _hasNext
                ? await GetBatchAsync(cancellationToken).ConfigureAwait(false)
                : null;

            _batchPos = 0;
        }

        /// <summary>
        /// Gets the next batch.
        /// </summary>
        protected abstract T[] GetBatch();

        /// <summary>
        /// Gets the next batch.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token observed before the batch is fetched.</param>
        protected abstract ValueTask<T[]> GetBatchAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Converter for GET_ALL operation.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Result.</returns>
        protected IList<T> ConvertGetAll(IBinaryStream stream)
        {
            var reader = _marsh.StartUnmarshal(stream, _keepBinary);

            var size = reader.ReadInt();

            var res = new List<T>(size);

            for (var i = 0; i < size; i++)
                res.Add(_readFunc(reader));

            return res;
        }

        /// <summary>
        /// Converter for GET_BATCH operation.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Result.</returns>
        protected T[] ConvertGetBatch(IBinaryStream stream)
        {
            var reader = _marsh.StartUnmarshal(stream, _keepBinary);

            var size = reader.ReadInt();

            if (size == 0)
            {
                _hasNext = false;
                return null;
            }

            var res = new T[size];

            for (var i = 0; i < size; i++)
            {
                res[i] = _readFunc(reader);
            }

            _hasNext = stream.ReadBool();

            return res;
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            lock (_syncRoot)
            {
                if (_disposed)
                {
                    return;
                }

                // When _hasNext is false, cursor is already disposed by the server.
                if (_hasNext)
                {
                    Dispose(true);
                }

                GC.SuppressFinalize(this);

                _disposed = true;
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "GC.SuppressFinalize is valid in DisposeAsync; the analyzer only recognizes Dispose.")]
        public async ValueTask DisposeAsync()
        {
            Task task = null;

            lock (_syncRoot)
            {
                if (_disposed)
                {
                    return;
                }

                // When _hasNext is false, cursor is already disposed by the server.
                if (_hasNext)
                {
                    // Initiate the close under the lock; await its completion without blocking a thread.
                    task = DisposeAsyncCore();
                }

                GC.SuppressFinalize(this);

                _disposed = true;
            }

            if (task != null)
            {
                await task.ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Releases the server-side cursor and returns a task that completes when the resource is closed.
        /// The default implementation has no asynchronous counterpart and falls back to the synchronous
        /// <see cref="Dispose(bool)"/>.
        /// </summary>
        protected virtual Task DisposeAsyncCore()
        {
            Dispose(true);

            return TaskRunner.CompletedTask;
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing">
        /// <c>true</c> when called from Dispose;  <c>false</c> when called from finalizer.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            // No-op.
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if this instance has been disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().Name, "Object has been disposed.");
            }
        }
    }
}

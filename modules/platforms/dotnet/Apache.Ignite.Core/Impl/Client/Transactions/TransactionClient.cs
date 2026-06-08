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

namespace Apache.Ignite.Core.Impl.Client.Transactions
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Transactions;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Ignite Thin Client transaction facade.
    /// </summary>
    internal class TransactionClient : ITransactionClient
    {
        /** Unique  transaction ID.*/
        private readonly int _id;

        /** Socket. */
        private readonly ClientSocket _socket;

        /** Transaction is closed. */
        private bool _closed;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">ID.</param>
        /// <param name="socket">Socket.</param>
        /// <param name="concurrency">Concurrency.</param>
        /// <param name="isolation">Isolation.</param>
        /// <param name="timeout">Timeout.</param>
        /// <param name="label">Label.</param>
        public TransactionClient(int id,
            ClientSocket socket,
            TransactionConcurrency concurrency,
            TransactionIsolation isolation,
            TimeSpan timeout,
            string? label)
        {
            _id = id;
            _socket = socket;
            Concurrency = concurrency;
            Isolation = isolation;
            Timeout = timeout;
            Label = label;
        }

        /** <inheritdoc /> */
        public void Commit()
        {
            ThrowIfClosed();
            Close(true);
        }

        /** <inheritdoc /> */
        public void Rollback()
        {
            ThrowIfClosed();
            Close(false);
        }

        /** <inheritdoc /> */
        public TransactionConcurrency Concurrency { get; private set; }

        /** <inheritdoc /> */
        public TransactionIsolation Isolation { get; private set; }

        /** <inheritdoc /> */
        public TimeSpan Timeout { get; private set; }

        /** <inheritdoc /> */
        public string? Label { get; private set; }

        /** <inheritdoc /> */
        public void Dispose()
        {
            try
            {
                Close(false);
            }
            catch
            {
                if (!_socket.IsDisposed)
                {
                    throw;
                }
            }
            finally
            {
                GC.SuppressFinalize(this);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "GC.SuppressFinalize is valid in DisposeAsync; the analyzer only recognizes Dispose.")]
        public async ValueTask DisposeAsync()
        {
            try
            {
                await CloseAsync(false).ConfigureAwait(false);
            }
            catch
            {
                if (!_socket.IsDisposed)
                {
                    throw;
                }
            }
            finally
            {
                GC.SuppressFinalize(this);
            }
        }

        /// <summary>
        /// Transaction Id.
        /// </summary>
        public int Id
        {
            get { return _id; }
        }

        public ClientSocket Socket
        {
            get
            {
                if (_socket.IsDisposed)
                {
                    throw new IgniteClientException("Transaction context has been lost due to connection errors.");
                }

                return _socket;
            }
        }

        /// <summary>
        /// Returns if transaction is closed.
        /// </summary>
        internal bool Closed
        {
            get { return _closed; }
        }

        /// <summary>
        /// Closes the transaction.
        /// </summary>
        private void Close(bool commit)
        {
            if (!_closed)
            {
                try
                {
                    Socket.DoOutInOp<object>(ClientOp.TxEnd,
                        ctx =>
                        {
                            ctx.Writer.WriteInt(_id);
                            ctx.Writer.WriteBoolean(commit);
                        },
                        null);
                }
                finally
                {
                    _closed = true;
                }
            }
        }

        /// <summary>
        /// Closes the transaction asynchronously.
        /// </summary>
        private Task CloseAsync(bool commit)
        {
            if (_closed)
            {
                return TaskRunner.CompletedTask;
            }

            // Mark as closed up front (matches the synchronous Close, which sets the flag even on failure).
            _closed = true;

            return Socket.DoOutInOpAsync<object?>(ClientOp.TxEnd,
                ctx =>
                {
                    ctx.Writer.WriteInt(_id);
                    ctx.Writer.WriteBoolean(commit);
                },
                _ => null);
        }

        /// <summary>
        /// Throws and exception if transaction is closed.
        /// </summary>
        private void ThrowIfClosed()
        {
            if (_closed)
            {
                throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
                    "Transaction {0} is closed",
                    Id));
            }
        }

        /** <inheritdoc /> */
        ~TransactionClient()
        {
            Dispose();
        }
    }
}

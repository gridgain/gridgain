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

namespace Apache.Ignite.Core.Impl.Client.Cache.Query
{
    using System;
    using Apache.Ignite.Core.Client.Cache.Query.Continuous;

    /// <summary>
    /// Thin client continuous query handle.
    /// </summary>
    internal sealed class ClientContinuousQueryHandle : IContinuousQueryHandleClient
    {
        /** Socket. */
        private readonly ClientSocket _socket;

        /** Cursor ID. */
        private readonly long _queryId;

        /** */
        private readonly object _disposeSyncRoot = new object();

        /** */
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of <see cref="ClientContinuousQueryHandle"/>.
        /// </summary>
        public ClientContinuousQueryHandle(ClientSocket socket, long queryId)
        {
            _socket = socket;
            _queryId = queryId;
        }

        /** <inheritdoc /> */
        public event EventHandler<ContinuousQueryClientDisconnectedEventArgs> Disconnected;

        /** <inheritdoc /> */
        public void Dispose()
        {
            lock (_disposeSyncRoot)
            {
                if (_disposed)
                {
                    return;
                }

                if (!_socket.IsDisposed)
                {
                    _socket.DoOutInOp<object>(ClientOp.ResourceClose,
                        ctx => ctx.Writer.WriteLong(_queryId), null);

                    _socket.RemoveNotificationHandler(_queryId);
                }

                _disposed = true;
            }
        }

        /// <summary>
        /// Called when error occurs during the continuous query execution.
        /// </summary>
        internal void OnError(Exception exception)
        {
            lock (_disposeSyncRoot)
            {
                if (_disposed)
                {
                    return;
                }

                var disconnected = Disconnected;
                if (disconnected != null)
                {
                    disconnected.Invoke(this, new ContinuousQueryClientDisconnectedEventArgs(exception));
                }

                _disposed = true;
            }
        }
    }
}

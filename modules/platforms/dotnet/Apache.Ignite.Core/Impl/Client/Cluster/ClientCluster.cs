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


namespace Apache.Ignite.Core.Impl.Client.Cluster
{
    using System;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Ignite client cluster implementation.
    /// </summary>
    internal class ClientCluster : IClientCluster
    {
        /** Attribute: platform. */
        private const string AttrPlatform = "org.apache.ignite.platform";

        /** Platform. */
        private const string Platform = "dotnet";

        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Cluster object pointer. */
        private readonly long _ptr;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Disposed flag. */
        private volatile bool _disposed;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="ptr">Remote cluster object pointer.</param>
        public ClientCluster(IgniteClient ignite, Marshaller marsh, long ptr)
        {
            _ignite = ignite;
            _marsh = marsh;
            _ptr = ptr;
        }

        /** <inheritdoc /> */
        public IClientClusterGroup ForAttribute(string name, string val)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            ThrowIfDisposed();

            Action<BinaryWriter> action = w =>
            {
                w.WriteString(name);
                w.WriteString(val);
            };
            var newPtr = DoOutInOp(ClientOp.ClusterForAttributes, action, r => r.ReadLong());
            return new ClientCluster(_ignite, _marsh, newPtr);
        }

        /** <inheritdoc /> */
        public IClientClusterGroup ForDotNet()
        {
            return ForAttribute(AttrPlatform, Platform);
        }

        /** <inheritdoc /> */
        public void SetActive(bool isActive)
        {
            ThrowIfDisposed();

            DoOutInOp(ClientOp.ClusterChangeState, w => w.WriteBoolean(isActive), r => r.ReadBool());
        }

        /** <inheritdoc /> */
        public bool IsActive()
        {
            ThrowIfDisposed();

            return DoOutInOp(ClientOp.ClusterIsActive, null, r => r.ReadBool());
        }

        /** <inheritdoc /> */
        public bool DisableWal(string cacheName)
        {
            ThrowIfDisposed();

            Action<BinaryWriter> action = w =>
            {
                w.WriteString(cacheName);
                w.WriteBoolean(false);
            };
            return DoOutInOp(ClientOp.ClusterChangeWalState, action, r => r.ReadBool());
        }

        /** <inheritdoc /> */
        public bool EnableWal(string cacheName)
        {
            ThrowIfDisposed();

            Action<BinaryWriter> action = w =>
            {
                w.WriteString(cacheName);
                w.WriteBoolean(true);
            };
            return DoOutInOp(ClientOp.ClusterChangeWalState, action, r => r.ReadBool());
        }

        /** <inheritdoc /> */
        public bool IsWalEnabled(string cacheName)
        {
            ThrowIfDisposed();

            return DoOutInOp(ClientOp.ClusterGetWalState, w => w.WriteString(cacheName), r => r.ReadBool());
        }

        /// <summary>
        /// Does the out in op.
        /// </summary>
        private T DoOutInOp<T>(ClientOp opId, Action<BinaryWriter> writeAction, Func<IBinaryStream, T> readFunc)
        {
            return _ignite.Socket.DoOutInOp(opId, stream => WriteRequest(writeAction, stream),
                readFunc, HandleError<T>);
        }

        /// <summary>
        /// Writes the request.
        /// </summary>
        private void WriteRequest(Action<BinaryWriter> writeAction, IBinaryStream stream)
        {
            stream.WriteLong(_ptr);

            if (writeAction != null)
            {
                var writer = _marsh.StartMarshal(stream);

                writeAction(writer);

                _marsh.FinishMarshal(writer);
            }
        }

        /// <summary>
        /// Handles the error.
        /// </summary>
        private T HandleError<T>(ClientStatusCode status, string msg)
        {
            throw new IgniteClientException(msg, null, status);
        }


        /** <inheritdoc /> */
        public void Dispose()
        {
            lock (this)
            {
                if (_disposed)
                {
                    return;
                }

                ReleaseUnmanagedResources();
                GC.SuppressFinalize(this);
                _disposed = true;
            }
        }

        /// <summary>
        /// Releases unmanaged resources.
        /// </summary>
        private void ReleaseUnmanagedResources()
        {
            try
            {
                DoOutInOp<object>(ClientOp.ResourceClose, w => w.WriteLong(_ptr), null);
            }
            finally
            {
                //no op
            }
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

        /// <summary>
        /// Finalizer.
        /// </summary>
        ~ClientCluster()
        {
            ReleaseUnmanagedResources();
        }

        /// <summary>
        /// Get simple copy of object for testing purposes.
        /// </summary>
        internal ClientCluster GetCopyForUnitTesting()
        {
            return new ClientCluster(_ignite, _marsh, _ptr);
        }
    }
}

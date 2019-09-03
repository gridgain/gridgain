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

        /** Cluster pointer. */
        private readonly long _ptr;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
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
            Action<BinaryWriter> action = writer =>
            {
                writer.WriteString(name);
                writer.WriteString(val);
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
            DoOutInOp(ClientOp.ClusterChangeState, w => w.WriteBoolean(isActive), r => r.ReadBool());
        }

        /** <inheritdoc /> */
        public bool IsActive()
        {
            return DoOutInOp(ClientOp.ClusterIsActive, null, r => r.ReadBool());
        }

        /** <inheritdoc /> */
        public bool DisableWal(string cacheName)
        {
            Action<BinaryWriter> action = writer =>
            {
                writer.WriteString(cacheName);
                writer.WriteBoolean(false);
            };
            return DoOutInOp(ClientOp.ClusterChangeWalState, action, r => r.ReadBool());
        }

        /** <inheritdoc /> */
        public bool EnableWal(string cacheName)
        {
            Action<BinaryWriter> action = writer =>
            {
                writer.WriteString(cacheName);
                writer.WriteBoolean(true);
            };
            return DoOutInOp(ClientOp.ClusterChangeWalState, action, r => r.ReadBool());
        }

        public bool IsWalEnabled(string cacheName)
        {
            return DoOutInOp(ClientOp.ClusterGetWalState, w => w.WriteString(cacheName), r => r.ReadBool());
        }

        /// <summary>
        /// Does the out in op.
        /// </summary>
        private T DoOutInOp<T>(ClientOp opId, Action<BinaryWriter> writeAction,
            Func<IBinaryStream, T> readFunc)
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

        /// <summary>
        /// Close resource during GC
        /// </summary>
        ~ClientCluster()
        {
            try
            {
                DoOutInOp<object>(ClientOp.ResourceClose, w => w.WriteLong(_ptr), null);
            }
            finally
            {
                //no ops
            }
        }
    }
}

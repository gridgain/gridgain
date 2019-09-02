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

    /// <summary>
    /// Ignite client cluster implementation.
    /// </summary>
    internal class ClientCluster : IClientCluster
    {
        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Cluster pointer. */
        private readonly long _ptr;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        /// <param name="ptr">Remote cluster object pointer.</param>
        public ClientCluster(IgniteClient ignite, long ptr)
        {
            _ignite = ignite;
            _ptr = ptr;
        }

        /** <inheritdoc /> */
        public IClientClusterGroup ForAttribute(string name, string val)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public IClientClusterGroup ForDataNodes(string name)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public IClientClusterGroup ForDotNet()
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public void SetActive(bool isActive)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public bool IsActive()
        {
            return _ignite.Socket.DoOutInOp(ClientOp.ClusterIsActivate, w => w.WriteLong(_ptr), s => s.ReadBool());
        }

        /** <inheritdoc /> */
        public void DisableWal(string cacheName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public void EnableWal(string cacheName)
        {
            throw new NotImplementedException();
        }
    }
}

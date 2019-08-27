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

    internal class ClientCluster : IClientCluster
    {
        private readonly long _ptr;

        public ClientCluster(long ptr)
        {
            _ptr = ptr;
        }

        public IClientClusterGroup ForAttribute(string name, string val)
        {
            throw new NotImplementedException();
        }

        public IClientClusterGroup ForDataNodes(string name)
        {
            throw new NotImplementedException();
        }

        public IClientClusterGroup ForDotNet()
        {
            throw new NotImplementedException();
        }

        public void SetActive(bool isActive)
        {
            throw new NotImplementedException();
        }

        public bool IsActive()
        {
            throw new NotImplementedException();
        }

        public void DisableWal(string cacheName)
        {
            throw new NotImplementedException();
        }

        public void EnableWal(string cacheName)
        {
            throw new NotImplementedException();
        }
    }
}

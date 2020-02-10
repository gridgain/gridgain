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

namespace Apache.Ignite.Core.Impl.Cache.Near
{
    using Apache.Ignite.Core.Cache.Affinity;

    /// <summary>
    /// Near cache entry.
    /// </summary>
    /// <typeparam name="T">Value type.</typeparam>
    internal class NearCacheEntry<T>
    {
        private readonly T _val;

        private readonly AffinityTopologyVersion _version;

        private readonly int _partition;

        public NearCacheEntry(T val, AffinityTopologyVersion version, int partition)
        {
            _val = val;
            _version = version;
            _partition = partition;
        }

        public T Val
        {
            get { return _val; }
        }

        public AffinityTopologyVersion Version
        {
            get { return _version; }
        }

        public int Partition
        {
            get { return _partition; }
        }
    }
}
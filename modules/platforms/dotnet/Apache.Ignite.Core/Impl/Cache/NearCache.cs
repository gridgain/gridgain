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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Holds near cache data for a given cache, serves one or more <see cref="CacheImpl{TK,TV}"/> instances.
    /// </summary>
    internal class NearCache<TK, TV> : INearCache, INearCache<TK, TV>
    {
        // TODO: Init capacity from settings
        // TODO: Eviction
        // TODO: Is it ok to use .NET-based comparison here, because it differs from Java-based comparison for keys?
        private readonly ConcurrentDictionary<TK, NearCacheEntry<TV>> _map = 
            new ConcurrentDictionary<TK, NearCacheEntry<TV>>();

        public bool TryGetValue(TK key, out TV val)
        {
            NearCacheEntry<TV> entry;

            if (_map.TryGetValue(key, out entry) && entry.HasValue)
            {
                val = entry.Value;
                return true;
            }

            val = default(TV);
            return false;
        }

        public void Put(TK key, TV val)
        {
            // TODO: Eviction according to limits.
            // TODO: Is eviction handled by Java side and a callback is going to be fired?
            _map[key] = new NearCacheEntry<TV>(true, val);
        }

        public INearCacheEntry<TV> GetOrCreateEntry(TK key)
        {
            return _map.GetOrAdd(key, _ => new NearCacheEntry<TV>());
        }

        public void Update(IBinaryStream keyStream, Marshaller marshaller)
        {
            Debug.Assert(keyStream != null);
            Debug.Assert(marshaller != null);

            var reader = marshaller.StartUnmarshal(keyStream);
            
            var key = reader.Deserialize<TK>();

            if (reader.ReadBoolean())
            {
                var val = reader.Deserialize<TV>();
                _map[key] = new NearCacheEntry<TV>(true, val);
            }
            else
            {
                NearCacheEntry<TV> unused;
                _map.TryRemove(key, out unused);
            }
        }

        public void Remove(TK key)
        {
            NearCacheEntry<TV> unused;
            _map.TryRemove(key, out unused);
        }
    }
}

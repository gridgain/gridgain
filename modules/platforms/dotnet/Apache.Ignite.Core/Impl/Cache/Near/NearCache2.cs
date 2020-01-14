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
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.IO;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Memory;

    /// <summary>
    /// Holds near cache data for a given cache, serves one or more <see cref="CacheImpl{TK,TV}"/> instances.
    /// </summary>
    internal sealed class NearCache2<TK, TV>
    {
        // TODO: Init capacity from settings
        // TODO: Eviction
        private volatile ConcurrentDictionary<TK, NearCacheEntry<TV>> _map = 
            new ConcurrentDictionary<TK, NearCacheEntry<TV>>();

        private volatile ConcurrentDictionary<object, NearCacheEntry<object>> _fallbackMap;

        public bool TryGetValue<TKey, TVal>(TKey key, out TVal val)
        {
            // ReSharper disable once SuspiciousTypeConversion.Global (reviewed)
            var map = _map as ConcurrentDictionary<TKey, NearCacheEntry<TVal>>;
            if (map != null)
            {
                NearCacheEntry<TVal> entry;
                if (map.TryGetValue(key, out entry) && entry.HasValue)
                {
                    val = entry.Value;
                    return true;
                }
            }
            
            if (_fallbackMap != null)
            {
                NearCacheEntry<object> fallbackEntry;
                if (_fallbackMap.TryGetValue(key, out fallbackEntry) && fallbackEntry.HasValue)
                {
                    val = (TVal) fallbackEntry.Value;
                    return true;
                }
            }

            val = default(TVal);
            return false;
        }

        public void Put<TKey, TVal>(TKey key, TVal val)
        {
            // TODO: Eviction according to limits.
            // Eviction callbacks from Java work for 2 out of 3 cases:
            // + Client node (all keys)
            // + Server node (non-primary keys)
            // - Server node (primary keys) - because there is no need to store primary keys in near cache
            // We can just ignore the third case and never evict primary keys - after all, we are on a server node,
            // and it is fine to keep primary keys in memory.

            // ReSharper disable once SuspiciousTypeConversion.Global (reviewed)
            var map = _map as ConcurrentDictionary<TKey, NearCacheEntry<TVal>>;
            if (map != null)
            {
                map[key] = new NearCacheEntry<TVal>(true, val);
                return;
            }

            EnsureFallbackMap();
            _fallbackMap[key] = new NearCacheEntry<object>(true, val);
        }

        public INearCacheEntry<TVal> GetOrCreateEntry<TKey, TVal>(TKey key)
        {
            // ReSharper disable once SuspiciousTypeConversion.Global (reviewed)
            var map = _map as ConcurrentDictionary<TKey, NearCacheEntry<TVal>>;
            if (map != null)
            {
                return map.GetOrAdd(key, _ => new NearCacheEntry<TVal>());
            }
            
            EnsureFallbackMap();
            var entry = _fallbackMap.GetOrAdd(key, _ => new NearCacheEntry<object>());
            return new NearCacheEntryGenericWrapper<TVal>(entry);
        }

        public void Update(IBinaryStream stream, Marshaller marshaller)
        {
            Debug.Assert(stream != null);
            Debug.Assert(marshaller != null);

            var pos = stream.Position;
            var reader = marshaller.StartUnmarshal(stream);

            var map = _map;
            if (map != null)
            {
                try
                {
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
                catch (InvalidCastException)
                {
                    // Ignore.
                    // _fallbackMap use case is not recommended, we expect this to be rare.
                }
            }

            if (_fallbackMap != null)
            {
                stream.Seek(pos, SeekOrigin.Begin);
                var key = reader.Deserialize<object>();

                if (reader.ReadBoolean())
                {
                    var val = reader.Deserialize<object>();
                    _fallbackMap[key] = new NearCacheEntry<object>(true, val);
                }
                else
                {
                    NearCacheEntry<object> unused;
                    _fallbackMap.TryRemove(key, out unused);
                }
            }
        }

        public void Evict(PlatformMemoryStream stream, Marshaller marshaller)
        {
            Debug.Assert(stream != null);
            Debug.Assert(marshaller != null);
            
            var pos = stream.Position;

            var map = _map;
            if (map != null)
            {
                try
                {
                    var key = marshaller.Unmarshal<TK>(stream);
                    NearCacheEntry<TV> unused;
                    _map.TryRemove(key, out unused);
                }
                catch (InvalidCastException)
                {
                    // Ignore.
                    // _fallbackMap use case is not recommended, we expect this to be rare.
                }
            }
            
            if (_fallbackMap != null)
            {
                stream.Seek(pos, SeekOrigin.Begin);
                var key = marshaller.Unmarshal<object>(stream);
                
                NearCacheEntry<object> unused;
                _fallbackMap.TryRemove(key, out unused);
            }
        }

        public void Clear()
        {
            if (_fallbackMap != null)
            {
                _fallbackMap.Clear();
            }
            else
            {
                var map = _map;
                if (map != null)
                {
                    map.Clear();
                }
            }
        }

        public void Remove<TKey, TVal>(TKey key)
        {
            // ReSharper disable once SuspiciousTypeConversion.Global (reviewed)
            var map = _map as ConcurrentDictionary<TKey, NearCacheEntry<TVal>>;
            if (map != null)
            {
                NearCacheEntry<TVal> unused;
                map.TryRemove(key, out unused);
            }

            if (_fallbackMap != null)
            {
                NearCacheEntry<object> unused;
                _fallbackMap.TryRemove(key, out unused);
            }
        }

        /// <summary>
        /// Switches this instance to fallback mode (generic downgrade).
        /// </summary>
        private void EnsureFallbackMap()
        {
            _fallbackMap = _fallbackMap ?? new ConcurrentDictionary<object, NearCacheEntry<object>>();
            _map = null;
        }
    }
}

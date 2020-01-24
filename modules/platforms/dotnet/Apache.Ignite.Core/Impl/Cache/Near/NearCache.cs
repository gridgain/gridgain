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
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Memory;

    /// <summary>
    /// Holds near cache data for a given cache, serves one or more <see cref="CacheImpl{TK,TV}"/> instances.
    /// </summary>
    internal sealed class NearCache<TK, TV> : INearCache
    {
        /** Generic map, used by default, should fit most use cases. */
        private volatile ConcurrentDictionary<TK, NearCacheEntry<TV>> _map = 
            new ConcurrentDictionary<TK, NearCacheEntry<TV>>();

        /** Non-generic map. Switched to when same cache is used with different generic arguments.
         * Less efficient because of boxing and casting. */
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

            var reader = marshaller.StartUnmarshal(stream);

            var key = reader.ReadObject<object>();
            var hasVal = reader.ReadBoolean();
            var val = hasVal ? reader.ReadObject<object>() : null;

            var map = _map;
            if (map != null && key is TK)
            {
                if (hasVal)
                {
                    if (val is TV)
                    {
                        _map[(TK)key] = new NearCacheEntry<TV>(true, (TV) val);
                    }
                }
                else
                {
                    NearCacheEntry<TV> unused;
                    _map.TryRemove((TK) key, out unused);
                }
            }

            if (_fallbackMap != null)
            {
                if (hasVal)
                {
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
            // Eviction callbacks from Java work for 2 out of 3 cases:
            // + Client node (all keys)
            // + Server node (non-primary keys)
            // - Server node (primary keys) - because there is no need to store primary keys in near cache

            Debug.Assert(stream != null);
            Debug.Assert(marshaller != null);
            
            var reader = marshaller.StartUnmarshal(stream);
            var key = reader.ReadObject<object>();

            var map = _map;
            if (map != null && key is TK)
            {
                NearCacheEntry<TV> unused;
                _map.TryRemove((TK) key, out unused);
            }

            if (_fallbackMap != null)
            {
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

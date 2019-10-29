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
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Manages <see cref="NearCache{TK,TV}"/> instances.
    /// Multiple <see cref="CacheImpl{TK,TV}"/> instances can exist for a given cache, and all of them share the same
    /// <see cref="NearCache{TK,TV}"/> instance.
    /// </summary>
    internal class NearCacheManager
    {
        /** TODO: Use weak references? Java keeps near cache data forever... */
        private readonly CopyOnWriteConcurrentDictionary<int, INearCache> _nearCaches
            = new CopyOnWriteConcurrentDictionary<int, INearCache>();

        /// <summary>
        /// Gets the near cache.
        /// <para />
        /// Same Ignite cache can be retrieved with different generic type parameters, e.g.:
        /// Ignite.GetCache{int, int}("my-cache") and Ignite.GetCache{string, string}("my-cache").
        /// This is not recommended, and we assume it does not usually happen, so we optimize for only one set of
        /// type parameters, which avoids boxing and increased memory usage.
        ///
        /// When the use case above is detected, we downgrade near cache map to {object, object}, which will cause
        /// more boxing and casting. 
        /// </summary>
        public INearCache<TK, TV> GetNearCache<TK, TV>(string cacheName,
            NearCacheConfiguration nearCacheConfiguration)
        {
            Debug.Assert(!string.IsNullOrEmpty(cacheName));
            Debug.Assert(nearCacheConfiguration != null);

            var cacheId = BinaryUtils.GetCacheId(cacheName);

            var nearCache = _nearCaches.GetOrAdd(cacheId, id => new NearCache<TK, TV>());

            var genericNearCache = nearCache as NearCache<TK, TV>;
            if (genericNearCache != null)
            {
                return genericNearCache;
            }

            var nonGenericNearCache = nearCache as NearCache<object, object>;
            if (nonGenericNearCache == null)
            {
                nonGenericNearCache = new NearCache<object, object>();
                _nearCaches.Set(cacheId, nonGenericNearCache);
            }

            return new NearCacheGenericWrapper<TK, TV>(nonGenericNearCache);
        }

        /// <summary>
        /// Reads cache entry from a stream and updates the near cache.
        /// </summary>
        public void Update(int cacheId, IBinaryStream stream, Marshaller marshaller)
        {
            INearCache nearCache;
            if (!_nearCaches.TryGetValue(cacheId, out nearCache))
            {
                return;
            }
            
            nearCache.Update(stream, marshaller);
        }
    }
}

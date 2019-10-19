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
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Manages <see cref="NearCache{TK,TV}"/> instances.
    /// Multiple <see cref="CacheImpl{TK,TV}"/> instances can exist for a given cache, and all of them share the same
    /// <see cref="NearCache{TK,TV}"/> instance.
    /// </summary>
    internal class NearCacheManager
    {
        /** TODO: Use weak references? Java keeps near cache data forever... */
        private readonly ConcurrentDictionary<Tuple<int, Type, Type>, WeakReference> _nearCaches
            = new ConcurrentDictionary<Tuple<int, Type, Type>, WeakReference>();

        /// <summary>
        /// Gets the near cache.
        /// </summary>
        public NearCache<TK, TV> GetNearCache<TK, TV>(string cacheName,
            NearCacheConfiguration nearCacheConfiguration)
        {
            Debug.Assert(!string.IsNullOrEmpty(cacheName));
            Debug.Assert(nearCacheConfiguration != null);

            var cacheId = BinaryUtils.GetCacheId(cacheName);
            
            return new NearCache<TK, TV>();
        }
    }
}

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
    using System.Collections.Generic;

    /// <summary>
    /// Manages <see cref="NearCache{TK,TV}"/> instances.
    /// Multiple <see cref="CacheImpl{TK,TV}"/> instances can exist for a given cache, and all of them share the same
    /// <see cref="NearCache{TK,TV}"/> instance.
    /// </summary>
    internal class NearCacheManager
    {
        /** */
        private readonly Dictionary<string, WeakReference> _nearCaches;

        /// <summary>
        /// Initializes a new instance of <see cref="NearCacheManager"/> class.
        /// </summary>
        public NearCacheManager()
        {
            // TODO: How do we remove near caches when underlying cache is destroyed? String name is not enough.
            // TODO: Use similar WeakReference mechanism as DataStreamer does
            _nearCaches = new Dictionary<string, WeakReference>();
        }

        public NearCache<TK, TV> GetNearCache<TK, TV>(string name)
        {
            // TODO: Return cache instance, set up continuous query.
            return new NearCache<TK, TV>();
        }
    }
}

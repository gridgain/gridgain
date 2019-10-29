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

    /// <summary>
    /// Generic wrapper over object-based near cache.
    /// </summary>
    internal class NearCacheGenericWrapper<TK, TV> : INearCache<TK, TV>
    {
        /** */
        private readonly NearCache<object, object> _nearCache;

        /// <summary>
        /// Initializes a new instance of <see cref="NearCacheGenericWrapper{K, V}"/> class.
        /// </summary>
        /// <param name="nearCache">Near cache to wrap.</param>
        public NearCacheGenericWrapper(NearCache<object, object> nearCache)
        {
            Debug.Assert(nearCache != null);
            
            _nearCache = nearCache;
        }

        /** <inheritdoc /> */
        public bool TryGetValue(TK key, out TV val)
        {
            object v;
            var res = _nearCache.TryGetValue(key, out v);
            
            val = res ? (TV) v : default(TV);
            
            return res;
        }

        /** <inheritdoc /> */
        public void Put(TK key, TV val)
        {
            _nearCache.Put(key, val);
        }

        /** <inheritdoc /> */
        public INearCacheEntry<TV> GetOrCreateEntry(TK key)
        {
            return new NearCacheEntryGenericWrapper<TV>(_nearCache.GetOrCreateEntry(key)); 
        }

        /** <inheritdoc /> */
        public void Remove(TK key)
        {
            _nearCache.Remove(key);
        }
    }
}
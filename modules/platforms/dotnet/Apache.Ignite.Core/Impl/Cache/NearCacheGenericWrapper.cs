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
    using System.Diagnostics;

    internal class NearCacheGenericWrapper<TK, TV> : INearCache<TK, TV>
    {
        private readonly NearCache<object, object> _nearCache;

        public NearCacheGenericWrapper(NearCache<object, object> nearCache)
        {
            Debug.Assert(nearCache != null);
            
            _nearCache = nearCache;
        }

        public bool TryGetValue(TK key, out TV val)
        {
            object v;
            var res = _nearCache.TryGetValue(key, out v);
            val = (TV) v;
            return res;
        }

        public void Put(TK key, TV val)
        {
            _nearCache.Put(key, val);
        }

        public INearCacheEntry<TV> GetOrCreateEntry(TK key)
        {
            return new NearCacheEntryGenericWrapper<TV>(_nearCache.GetOrCreateEntry(key)); 
        }

        public void Remove(TK key)
        {
            throw new System.NotImplementedException();
        }
    }
}
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
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    internal interface INearCache<in TK, TV>
    {
        bool TryGetValue(TK key, out TV val);
        
        void Put(TK key, TV val);
        
        INearCacheEntry<TV> GetOrCreateEntry(TK key);
        
        void Remove(TK key);
    }

    /// <summary>
    /// Non-generic near cache facade.
    /// </summary>
    internal interface INearCache
    {
        /// <summary>
        /// Reads cache key from a stream and invalidates.
        /// </summary>
        void Update(IBinaryStream keyStream, Marshaller marshaller);
    }
}
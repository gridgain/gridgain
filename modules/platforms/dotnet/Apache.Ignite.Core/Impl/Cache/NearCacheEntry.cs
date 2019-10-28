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
    using System.Threading;

    /// <summary>
    /// <see cref="NearCache{TK, TV}"/> entry.
    /// </summary>
    internal struct NearCacheEntry<T>
    {
        /** */
        private volatile int _hasValue;
        
        /** */
        private T _value;

        public NearCacheEntry(bool hasValue = false, T value = default(T))
        {
            _hasValue = 0;
            _value = value;
        }

        public bool HasValue
        {
            get { return _hasValue > 0; }
        }

        public T Value
        {
            get { return _value; }
        }

        public void SetValueIfEmpty(T value)
        {
            if (Interlocked.CompareExchange(ref _hasValue, 0, 1) == 0)
            {
                _value = value;
            }
        }
    }
}
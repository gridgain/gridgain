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
    /// <summary>
    /// Near cache entry.
    /// </summary>
    /// <typeparam name="T">Type of the value.</typeparam>
    internal interface INearCacheEntry<T>
    {
        /// <summary>
        /// Gets a value indicating whether this entry has a value.
        /// </summary>
        bool HasValue { get; }
        
        /// <summary>
        /// Gets the value.
        /// </summary>
        T Value { get; }
        
        /// <summary>
        /// Sets value atomically only if it has not been set before.
        /// </summary>
        /// <param name="value">Value.</param>
        void SetValueIfEmpty(T value);
    }
}
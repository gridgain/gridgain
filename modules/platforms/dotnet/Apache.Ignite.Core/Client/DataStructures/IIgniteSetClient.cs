/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

namespace Apache.Ignite.Core.Client.DataStructures
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Distributed set: stores items on one or more Ignite cluster nodes.
    /// <para />
    /// Implements most of the methods from <see cref="ISet{T}"/>. The following methods are NOT supported:
    /// <see cref="ISet{T}.IsSubsetOf"/>, <see cref="ISet{T}.IsProperSubsetOf"/>,
    /// <see cref="ISet{T}.Overlaps"/>, <see cref="ISet{T}.SymmetricExceptWith"/>.
    /// <para />
    /// Set items can be placed on single node (when <see cref="CollectionClientConfiguration.Colocated"/> is true)
    /// or distributed across grid nodes.
    /// </summary>
    /// <typeparam name="T">Item type.</typeparam>
    [SuppressMessage("Microsoft.Naming", "CA1710:IdentifiersShouldHaveCorrectSuffix")]
    public interface IIgniteSetClient<T> : ISet<T>
    {
        /// <summary>
        /// Gets the set name.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets a value indicating whether this set is colocated, that is, all items are stored on a single node.
        /// </summary>
        bool Colocated { get; }

        /// <summary>
        /// Gets or sets a value indicating the batch size for multi-item operations such as iteration.
        /// </summary>
        int PageSize { get; set; }

        /// <summary>
        /// Gets a value indicating whether this instance was removed from the cluster.
        /// </summary>
        /// <returns>True if this set was removed; otherwise, false.</returns>
        bool IsClosed { get; }

        /// <summary>
        /// Removes this set from the cluster.
        /// </summary>
        void Close();
    }
}

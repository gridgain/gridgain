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

namespace Apache.Ignite.Core.Client
{
    using System;

    /// <summary>
    /// Defines grid projection which represents a common functionality over a group of nodes.
    /// Grid projection allows to group Ignite nodes into various subgroups to perform distributed
    /// operations on them. All ForXXX(...)' methods will create a child grid projection
    /// from existing projection. If you create a new projection from current one, then the resulting
    /// projection will include a subset of nodes from current projection.
    /// </summary>
    public interface IClientClusterGroup : IDisposable
    {
        /// <summary>
        /// Creates projection for nodes containing given name and value
        /// specified in user attributes.
        /// </summary>
        /// <param name="name">Name of the attribute.</param>
        /// <param name="val">Optional attribute value to match.</param>
        /// <returns>Grid projection for nodes containing specified attribute.</returns>
        IClientClusterGroup ForAttribute(string name, string val);

        /// <summary>
        /// Creates grid projection for nodes supporting .Net, i.e. for nodes started with Apache.Ignite.exe.
        /// </summary>
        /// <returns>Grid projection for nodes supporting .Net.</returns>
        IClientClusterGroup ForDotNet();
    }
}

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

namespace Apache.Ignite.Core.Impl.Cluster
{
    using System;
    using Apache.Ignite.Core.Cluster;

    /// <summary>
    /// Thick client cluster node data provider.
    /// </summary>
    internal class ClusterNodeDataProvider : IClusterNodeDataProvider
    {
        /** Ignite reference. */
        private readonly WeakReference _igniteRef;

        /// <summary>
        /// Initializes an instance of <see cref="ClusterNodeDataProvider"/> with a grid.
        /// </summary>
        /// <param name="grid">Ignite grid.</param>
        public ClusterNodeDataProvider(Ignite grid)
        {
            _igniteRef = new WeakReference(grid);
        }

        /** <inheritDoc /> */
        public ClusterMetricsImpl GetMetrics(Guid nodeId, long lastUpdateTime)
        {
            var ignite = (Ignite)_igniteRef.Target;

            if (ignite == null)
                return null;

            return ignite.ClusterGroup.RefreshClusterNodeMetrics(nodeId, lastUpdateTime);
        }
    }
}

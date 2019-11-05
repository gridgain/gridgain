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

namespace Apache.Ignite.Core.Cluster
{
    using System;
    using Apache.Ignite.Core.Impl.Cluster;

    /// <summary>
    /// Provides cluster node details from remote server.
    /// </summary>
    internal interface IClusterNodeDataProvider
    {
        /// <summary>
        /// Gets metrics snapshot for a node.
        /// </summary>
        /// <param name="nodeId">Node Id.</param>
        /// /// <param name="lastUpdateTime">Last update time in raw format.</param>
        /// <returns>Runtime metrics snapshot for the node.
        /// <c>Null</c> if no new metrics available.</returns>
        ClusterMetricsImpl GetMetrics(Guid nodeId, long lastUpdateTime);
    }
}

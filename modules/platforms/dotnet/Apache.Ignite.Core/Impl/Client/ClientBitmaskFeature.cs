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

namespace Apache.Ignite.Core.Impl.Client
{
    /// <summary>
    /// Client feature ids. Values represent the index in the bit array.
    /// Unsupported flags must be commented out.
    /// </summary>
    internal enum ClientBitmaskFeature
    {
        // UserAttributes = 0,
        ExecuteTaskByName = 1,
        // ClusterStates = 2,
        ClusterGroupGetNodesEndpoints = 3,
        ClusterGroups = 4,
        ServiceInvoke = 5, // The flag is not necessary and exists for legacy reasons
        // DefaultQueryTimeout = 6, // IGNITE-13692
        QueryPartitionsBatchSize = 7,
        BinaryConfiguration = 8,
        ServiceInvokeCtx = 10,
        Heartbeat = 11,
        CachePluginConfigurations = 32
    }
}

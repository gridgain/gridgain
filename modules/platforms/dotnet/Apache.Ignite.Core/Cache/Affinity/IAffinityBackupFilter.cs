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

namespace Apache.Ignite.Core.Cache.Affinity
{
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;

    /// <summary>
    /// Represents a backup filter for an affinity function - see
    /// <see cref="RendezvousAffinityFunction.AffinityBackupFilter"/>.
    /// <para />
    /// Only one predefined implementation is supported for now: <see cref="ClusterNodeAttributeAffinityBackupFilter"/>.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1040:AvoidEmptyInterfaces")]
    public interface IAffinityBackupFilter
    {
        // No-op: custom implementations are not supported.
    }
}

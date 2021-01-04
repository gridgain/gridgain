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

package org.apache.ignite.internal.cluster;

import java.util.UUID;
import org.apache.ignite.IgniteCluster;

/**
 *
 */
public interface IgniteClusterEx extends IgniteCluster, ClusterGroupEx {
    /**
     * Cluster ID is a unique identifier automatically generated when cluster starts up for the very first time.
     *
     * It is a cluster-wide property so all nodes of the cluster (including client nodes) return the same value.
     *
     * In in-memory clusters ID is generated again upon each cluster restart.
     * In clusters running in persistent mode cluster ID is stored to disk and is used even after full cluster restart.
     *
     * @return Unique cluster ID.
     */
    public UUID id();
}
/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cluster;

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Request for the getting current cluster state from client node.
 */
@GridInternal
public class ClientGetClusterStateComputeRequest implements IgniteCallable<ClusterState> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Ignite. */
    @IgniteInstanceResource
    private Ignite ig;

    /** {@inheritDoc} */
    @Override public ClusterState call() throws Exception {
        return ig.cluster().state();
    }
}


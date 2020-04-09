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

package org.apache.ignite.spi.communication.tcp.internal;

import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;

/**
 * Registrator of events which will process in future.
 */
public class TimeObjectProcessorWrapper {
    /** Cluster state provider. */
    private final ClusterStateProvider clusterStateProvider;

    /**
     * @param clusterStateProvider Cluster state provider.
     */
    public TimeObjectProcessorWrapper(ClusterStateProvider clusterStateProvider) {
        this.clusterStateProvider = clusterStateProvider;
    }

    /**
     * @param obj Timeout object.
     * @see IgniteSpiContext#addTimeoutObject(IgniteSpiTimeoutObject)
     */
    protected void addTimeoutObject(IgniteSpiTimeoutObject obj) {
        clusterStateProvider.getSpiContext().addTimeoutObject(obj);
    }

    /**
     * @param obj Timeout object.
     * @see IgniteSpiContext#removeTimeoutObject(IgniteSpiTimeoutObject)
     */
    protected void removeTimeoutObject(IgniteSpiTimeoutObject obj) {
        clusterStateProvider.getSpiContext().removeTimeoutObject(obj);
    }
}

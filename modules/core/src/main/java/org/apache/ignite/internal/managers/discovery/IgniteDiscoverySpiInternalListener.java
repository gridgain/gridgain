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

package org.apache.ignite.internal.managers.discovery;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;

/**
 * For TESTING only.
 */
public interface IgniteDiscoverySpiInternalListener {
    /**
     * @param locNode Local node.
     * @param log Log.
     */
    public void beforeJoin(ClusterNode locNode, IgniteLogger log);

    /**
     * @param locNode Local node.
     * @param log Logger.
     */
    public default void beforeReconnect(ClusterNode locNode, IgniteLogger log) {
        // No-op.
    }

    /**
     * @param spi SPI instance.
     * @param log Logger.
     * @param msg Custom message.
     * @return {@code False} to cancel event send.
     */
    public boolean beforeSendCustomEvent(DiscoverySpi spi, IgniteLogger log, DiscoverySpiCustomMessage msg);
}

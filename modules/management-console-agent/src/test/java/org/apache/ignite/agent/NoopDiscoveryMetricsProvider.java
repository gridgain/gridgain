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

package org.apache.ignite.agent;

import java.util.Map;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;

/**
 * No-op discovery metrics provider.
 */
public class NoopDiscoveryMetricsProvider implements DiscoveryMetricsProvider {
    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, CacheMetrics> cacheMetrics() {
        return null;
    }
}

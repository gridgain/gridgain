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

package org.apache.ignite.console.metrics;

import java.util.Collection;
import java.util.UUID;

/**
 *
 */
public class MetricsDto {
    /** */
    private UUID nid;

    /** */
    private long ts;

    /** */
    private Collection<MetricDto> metrics;

    /**
     * Default constructor for serialization.
     */
    public MetricsDto() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param nid Node ID.
     * @param ts Metrics timestamp.
     * @param metrics Metrics.
     */
    public MetricsDto(UUID nid, long ts, Collection<MetricDto> metrics) {
        this.nid = nid;
        this.ts = ts;
        this.metrics = metrics;
    }

    /**
     * @return Node ID.
     */
    public UUID getNodeId() {
        return nid;
    }

    /**
     * @param nid Node ID.
     */
    public void setNodeId(UUID nid) {
        this.nid = nid;
    }

    /**
     * @return Metrics timestamp.
     */
    public long getTimestamp() {
        return ts;
    }

    /**
     * @param ts Metrics timestamp.
     */
    public void setTimestamp(long ts) {
        this.ts = ts;
    }

    /**
     * @return Metrics.
     */
    public Collection<MetricDto> getMetrics() {
        return metrics;
    }

    /**
     * @param metrics Metrics.
     */
    public void setMetrics(Collection<MetricDto> metrics) {
        this.metrics = metrics;
    }
}

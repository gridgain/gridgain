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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CachePartitionExchangeWorkerTask;

/**
 * A task for finishing preloading future in exchange worker thread.
 */
public class FinishPreloadingTask implements CachePartitionExchangeWorkerTask {
    /** Topology version. */
    private final AffinityTopologyVersion topVer;

    /** Group id. */
    private final int grpId;

    /** Rebalance id. */
    private final long rebalanceId;

    /**
     * @param topVer Topology version.
     */
    public FinishPreloadingTask(AffinityTopologyVersion topVer, int grpId, long rebalanceId) {
        this.grpId = grpId;
        this.topVer = topVer;
        this.rebalanceId = rebalanceId;
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean skipForExchangeMerge() {
        return true;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Group id.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * @return Rebalance id.
     */
    public long rebalanceId() {
        return rebalanceId;
    }
}

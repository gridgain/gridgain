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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Utility class for ClusterState* tests.
 */
class ClusterStateTestUtils {
    /** */
    static final String FAILED_ACTIVATE_MSG =
        "Failed to activate cluster (must invoke the method outside of an active transaction).";

    /** */
    static final String FAILED_DEACTIVATE_MSG =
        "Failed to deactivate cluster (must invoke the method outside of an active transaction).";

    /** */
    static final String FAILED_READ_ONLY_MSG =
        "Failed to activate cluster in read-only mode (must invoke the method outside of an active transaction).";

    /**
     * @param cacheName Cache name.
     * @return Partitioned cache configuration with 1 backup.
     */
    static CacheConfiguration partitionedCache(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Replicated cache configuration.
     */
    static CacheConfiguration replicatedCache(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return ccfg;
    }

    /**
     * @param ignite Cluster node.
     * @param state New cluster state.
     * @return Callable which tries to change cluster state to {@code state} from {@code ignite} node.
     */
    static Callable<Object> changeState(Ignite ignite, ClusterState state) {
        return () -> {
            ignite.cluster().state(state);

            return null;
        };
    }

    /** */
    private ClusterStateTestUtils() {
    }
}

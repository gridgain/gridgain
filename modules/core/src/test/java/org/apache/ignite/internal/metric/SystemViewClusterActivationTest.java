/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.metric;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODES_SYS_VIEW;
import static org.apache.ignite.internal.managers.systemview.ScanQuerySystemView.SCAN_QRY_SYS_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHE_GRPS_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager.DATA_REGION_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager.TXS_MON_LIST;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.CQ_SYS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LATCHES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LOCKS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LONGS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.QUEUES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.REFERENCES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SEMAPHORES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SEQUENCES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SETS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.STAMPED_VIEW;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.JOBS_VIEW;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLI_CONN_VIEW;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.STREAM_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.SYS_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TASKS_VIEW;

/** Tests for {@link SystemView}. */
public class SystemViewClusterActivationTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED", value = "true")
    public void testStartInactiveCluster() throws Exception {
        Set<String> expViews = new HashSet<>(Arrays.asList(
            CACHE_GRP_PAGE_LIST_VIEW,
            CACHE_GRPS_VIEW,
            CACHES_VIEW,
            CLI_CONN_VIEW,
            CQ_SYS_VIEW,
            DATA_REGION_PAGE_LIST_VIEW,
            STREAM_POOL_QUEUE_VIEW,
            JOBS_VIEW,
            NODES_SYS_VIEW,
            SCAN_QRY_SYS_VIEW,
            SVCS_VIEW,
            SYS_POOL_QUEUE_VIEW,
            TASKS_VIEW,
            TXS_MON_LIST,
            QUEUES_VIEW,
            SETS_VIEW,
            LOCKS_VIEW,
            SEMAPHORES_VIEW,
            LATCHES_VIEW,
            STAMPED_VIEW,
            REFERENCES_VIEW,
            LONGS_VIEW,
            SEQUENCES_VIEW
        ));

        IgniteEx ignite = startGrid(getConfiguration().setClusterStateOnStart(ClusterState.INACTIVE));

        ignite.cluster().state(ClusterState.ACTIVE);

        Set<String> views = StreamSupport.stream(ignite.context().systemView().spliterator(), false)
            .map(SystemView::name).collect(Collectors.toSet());

        assertTrue(views.containsAll(expViews));
    }
}

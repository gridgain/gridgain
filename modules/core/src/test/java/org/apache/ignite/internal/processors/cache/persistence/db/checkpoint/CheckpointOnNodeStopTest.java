/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_FORCED_CHECKPOINT_ON_NODE_STOP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Class for checking the behavior of a checkpoint on a node stop. */
public class CheckpointOnNodeStopTest extends GridCommonAbstractTest {
    /** */
    private static final String SHUTDOWN_REASON = "forcible at node stop";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        return super.getConfiguration(igniteInstanceName).setDataStorageConfiguration(dsCfg);
    }

    /**  */
    @Test
    public void testStopBeforeActivateWithoutSystemProperty() throws Exception {
        IgniteEx n = startGrid(0);

        Collection<CheckpointProgress> progresses = startListenForBeginCheckpoints(n);

        stopGridGracefully(0);

        assertThat(progresses, empty());
    }

    /**  */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_FORCED_CHECKPOINT_ON_NODE_STOP, value = "true")
    public void testStopBeforeActivateWithSystemProperty() throws Exception {
        testStopBeforeActivateWithoutSystemProperty();
    }

    /**  */
    @Test
    public void testStopAfterActivateWithoutSystemProperty() throws Exception {
        IgniteEx n = startGrid(0);

        Collection<CheckpointProgress> progresses = startListenForBeginCheckpoints(n);

        n.cluster().state(ClusterState.ACTIVE);

        stopGridGracefully(0);

        assertThat(reasons(progresses), not(hasItem(SHUTDOWN_REASON)));
        assertAllCheckpointsFinished(progresses);
    }

    /**  */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_FORCED_CHECKPOINT_ON_NODE_STOP, value = "true")
    public void testStopAfterActivateWithSystemProperty() throws Exception {
        IgniteEx n = startGrid(0);

        Collection<CheckpointProgress> progresses = startListenForBeginCheckpoints(n);

        n.cluster().state(ClusterState.ACTIVE);

        stopGridGracefully(0);

        assertThat(reasons(progresses), hasItem(SHUTDOWN_REASON));
        assertAllCheckpointsFinished(progresses);
    }

    /**  */
    @Test
    public void testStopWithCacheWithoutSystemProperty() throws Exception {
        IgniteEx n = startGrid(0);

        Collection<CheckpointProgress> progresses = startListenForBeginCheckpoints(n);

        n.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = n.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        stopGridGracefully(0);

        assertThat(reasons(progresses), not(hasItem(SHUTDOWN_REASON)));
        assertAllCheckpointsFinished(progresses);
    }

    /**  */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_FORCED_CHECKPOINT_ON_NODE_STOP, value = "true")
    public void testStopWithCacheWithSystemProperty() throws Exception {
        IgniteEx n = startGrid(0);

        Collection<CheckpointProgress> progresses = startListenForBeginCheckpoints(n);

        n.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = n.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        stopGridGracefully(0);

        assertThat(reasons(progresses), hasItem(SHUTDOWN_REASON));
        assertAllCheckpointsFinished(progresses);
    }

    /** */
    private void stopGridGracefully(int nodeIdx) {
        stopGrid(nodeIdx, true);
    }

    /**  */
    private Collection<CheckpointProgress> startListenForBeginCheckpoints(IgniteEx n) {
        Collection<CheckpointProgress> progresses = new ConcurrentLinkedQueue<>();

        dbMgr(n).addCheckpointListener(listenForBeginCheckpoints(progresses));

        return progresses;
    }

    /** */
    private static void assertAllCheckpointsFinished(Collection<CheckpointProgress> progresses) {
        int i = 0;

        for (CheckpointProgress progress : progresses) {
            assertTrue(
                    "i=" + i + ", reason=" + progress.reason(),
                    progress.futureFor(CheckpointState.FINISHED).isDone()
            );

            i++;
        }
    }

    /**  */
    private static List<String> reasons(Collection<CheckpointProgress> progresses) {
        return progresses.stream()
                .map(CheckpointProgress::reason)
                .collect(toList());
    }

    /**  */
    private static CheckpointListener listenForBeginCheckpoints(Collection<CheckpointProgress> progresses) {
        return new CheckpointListener() {
            /** {@inheritDoc} */
            @Override public void onMarkCheckpointBegin(Context ctx) {
                /** No-op. */
            }

            /** {@inheritDoc} */
            @Override public void onCheckpointBegin(Context ctx) {
                progresses.add(ctx.progress());
            }

            /** {@inheritDoc} */
            @Override public void beforeCheckpointBegin(Context ctx) {
                /** No-op. */
            }
        };
    }
}

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

package org.apache.ignite.internal.processors.compute;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.DFLT_TEST_TIMEOUT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Class for testing job priority change.
 */
public class ComputeJobChangePriorityTest extends GridCommonAbstractTest {
    /** Coordinator. */
    private static IgniteEx CRD;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        IgniteEx crd = startGrids(2);

        crd.cluster().state(ACTIVE);

        awaitPartitionMapExchange();

        CRD = crd;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        CRD = null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        for (Ignite n : G.allGrids())
            PriorityQueueCollisionSpiEx.spiEx(n).reset();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        // Enable collision handling to ensure that any passive WaitJobs will be handled and won't block subsequent tests.
        for (Ignite n : G.allGrids())
            PriorityQueueCollisionSpiEx.spiEx(n).handleCollision = true;

        // Release any WaitJobs already executing (blocked on waitFut.get()).
        WaitJob.waitFut.onDone();

        // Force collision handling to activate any passive WaitJobs.
        for (Ignite n : G.allGrids())
            ((IgniteEx)n).context().job().handleCollisions();

        assertTrue(waitForCondition(() -> CRD.compute().activeTaskFutures().isEmpty(), getTestTimeout()));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setCollisionSpi(new PriorityQueueCollisionSpiEx())
            .setMetricsUpdateFrequency(Long.MAX_VALUE)
            .setClientFailureDetectionTimeout(Long.MAX_VALUE);
    }

    /**
     * Checking that when {@link PriorityQueueCollisionSpi#getPriorityAttributeKey} is changed,
     * collisions will be handled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeTaskPriorityAttribute() throws Exception {
        checkChangeAttributes(
            PriorityQueueCollisionSpiEx.spiEx(CRD).getPriorityAttributeKey(),
            1,
            true
        );
    }

    /**
     * Checking that when {@link PriorityQueueCollisionSpi#getJobPriorityAttributeKey} is changed,
     * collisions will be handled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeJobPriorityAttribute() throws Exception {
        checkChangeAttributes(
            PriorityQueueCollisionSpiEx.spiEx(CRD).getJobPriorityAttributeKey(),
            1,
            true
        );
    }

    /**
     * Checking that no collision handling will occur when a random attribute is changed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangeRandomAttribute() throws Exception {
        checkChangeAttributes(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            false
        );
    }

    /** */
    private void checkChangeAttributes(
        String key,
        Object val,
        boolean expHandleCollisionOnChangeTaskAttrs
    ) throws Exception {
        WaitJob.waitFut.reset();

        ComputeTaskFuture<Void> taskFut = CRD.compute().executeAsync(new NoopTask(), null);

        for (Ignite n : G.allGrids())
            PriorityQueueCollisionSpiEx.spiEx(n).waitJobFut.get(getTestTimeout());

        for (Ignite n : G.allGrids())
            PriorityQueueCollisionSpiEx.spiEx(n).handleCollision = true;

        taskFut.getTaskSession().setAttribute(key, val);

        for (Ignite n : G.allGrids()) {
            assertEquals(
                val,
                PriorityQueueCollisionSpiEx.spiEx(n).waitJobFut.result()
                    .getTaskSession().waitForAttribute(key, getTestTimeout()));
        }

        WaitJob.waitFut.onDone();

        for (Ignite n : G.allGrids()) {
            GridFutureAdapter<Void> fut = PriorityQueueCollisionSpiEx.spiEx(n).onHandleCollisionFut;

            if (expHandleCollisionOnChangeTaskAttrs)
                fut.get(getTestTimeout());
            else
                assertThrows(log, () -> fut.get(100), IgniteFutureTimeoutCheckedException.class, null);
        }

        if (!expHandleCollisionOnChangeTaskAttrs)
            CRD.compute().execute(new NoopTask(), null);

        taskFut.get(getTestTimeout());
    }

    /** */
    private static class PriorityQueueCollisionSpiEx extends PriorityQueueCollisionSpi {
        /** */
        volatile boolean handleCollision;

        volatile GridFutureAdapter<CollisionJobContext> waitJobFut = new GridFutureAdapter<>();

        volatile GridFutureAdapter<Void> onHandleCollisionFut = new GridFutureAdapter<>();

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            GridFutureAdapter<CollisionJobContext> waitFut = waitJobFut;
            GridFutureAdapter<Void> handleCollisionFut = onHandleCollisionFut;

            if (handleCollision) {
                handleCollisionFut.onDone();

                super.onCollision(ctx);
            }

            if (!waitFut.isDone()) {
                ctx.waitingJobs().stream()
                    .filter(collisionJobCtx -> collisionJobCtx.getJob() instanceof WaitJob)
                    .findAny()
                    .ifPresent(waitFut::onDone);
            }
        }

        /** */
        void reset() {
            handleCollision = false;

            waitJobFut = new GridFutureAdapter<>();

            onHandleCollisionFut = new GridFutureAdapter<>();
        }

        /** */
        static PriorityQueueCollisionSpiEx spiEx(Ignite n) {
            return ((PriorityQueueCollisionSpiEx)((IgniteEx)n).context().config().getCollisionSpi());
        }
    }

    /** */
    @ComputeTaskSessionFullSupport
    private static class NoopTask extends ComputeTaskAdapter<Void, Void> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            Void arg
        ) throws IgniteException {
            return subgrid.stream().collect(toMap(n -> new WaitJob(), identity()));
        }

        /** {@inheritDoc} */
        @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }

    @Override protected long getTestTimeout() {
        return 30_000;
    }

    /** */
    private static class WaitJob extends ComputeJobAdapter {
        /** */
        static final GridFutureAdapter<Void> waitFut = new GridFutureAdapter<>();

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            try {
                waitFut.get(DFLT_TEST_TIMEOUT);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            return null;
        }
    }
}

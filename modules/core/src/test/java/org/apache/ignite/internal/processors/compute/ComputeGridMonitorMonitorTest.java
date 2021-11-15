/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.task.monitor.ComputeGridMonitor;
import org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusDiff;
import org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusSnapshot;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusEnum.FAILED;
import static org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusEnum.FINISHED;
import static org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusEnum.RUNNING;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Test class for {@link ComputeGridMonitor}.
 */
public class ComputeGridMonitorMonitorTest extends GridCommonAbstractTest {
    /** Coordinator. */
    private static IgniteEx CRD;

    /** Compute task status monitor. */
    private ComputeGridMonitorImpl monitor;

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
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        CRD.context().task().listenStatusUpdates(monitor = new ComputeGridMonitorImpl());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        CRD.context().task().stopListenStatusUpdates(monitor);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler());
    }

    /**
     * Checking get of diffs for the successful execution of the task.
     */
    @Test
    public void simpleTest() {
        ComputeTaskFuture<Void> taskFut = CRD.compute().executeAsync(new NoopComputeTask(), null);

        taskFut.get(getTestTimeout());

        assertTrue(monitor.statusSnapshots.isEmpty());

        assertEquals(3, monitor.statusDiffs.size());

        checkStartTask(monitor.statusDiffs.poll(), taskFut.getTaskSession());
        checkChangeJobNodes(monitor.statusDiffs.poll(), taskFut.getTaskSession());
        checkFinishTask(monitor.statusDiffs.poll(), taskFut.getTaskSession());
    }

    /**
     * Checking get of diffs for the failed execution of the task.
     */
    @Test
    public void failTaskTest() {
        NoopComputeTask task = new NoopComputeTask() {
            /** {@inheritDoc} */
            @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
                throw new IgniteException("FAIL TASK");
            }
        };

        ComputeTaskFuture<Void> taskFut = CRD.compute().executeAsync(task, null);

        assertThrows(log, () -> taskFut.get(getTestTimeout()), IgniteException.class, null);

        assertTrue(monitor.statusSnapshots.isEmpty());

        assertEquals(3, monitor.statusDiffs.size());

        checkStartTask(monitor.statusDiffs.poll(), taskFut.getTaskSession());
        checkChangeJobNodes(monitor.statusDiffs.poll(), taskFut.getTaskSession());
        checkFailTask(monitor.statusDiffs.poll(), taskFut.getTaskSession());
    }

    @Test
    public void changeAttributesTest() throws Exception {
        ComputeFullWithWaitTask task = new ComputeFullWithWaitTask(getTestTimeout());

        ComputeTaskFuture<Void> taskFut = CRD.compute().executeAsync(task, null);

        task.doneOnMapFut.get(getTestTimeout());

        taskFut.getTaskSession().setAttribute("test", "test");

        task.waitOnJobExecFut.onDone();

        taskFut.get(getTestTimeout());

        assertTrue(monitor.statusSnapshots.isEmpty());

        assertEquals(4, monitor.statusDiffs.size());

        checkStartTask(monitor.statusDiffs.poll(), taskFut.getTaskSession());
        checkChangeJobNodes(monitor.statusDiffs.poll(), taskFut.getTaskSession());
        checkChangeAttributesNodes(monitor.statusDiffs.poll(), taskFut.getTaskSession());
        checkFinishTask(monitor.statusDiffs.poll(), taskFut.getTaskSession());
    }

    /**
     * Checking the get of snapshots of task statuses.
     *
     * @throws Exception If failed.
     */
    @Test
    public void snapshotsTest() throws Exception {
        ComputeFullWithWaitTask task = new ComputeFullWithWaitTask(getTestTimeout());

        ComputeTaskFuture<Void> taskFut = CRD.compute().executeAsync(task, null);

        task.doneOnMapFut.get(getTestTimeout());

        ComputeGridMonitorImpl monitor1 = new ComputeGridMonitorImpl();

        try {
            CRD.context().task().listenStatusUpdates(monitor1);

            task.waitOnJobExecFut.onDone();

            assertTrue(monitor.statusSnapshots.isEmpty());

            assertEquals(1, monitor1.statusSnapshots.size());

            checkSnapshot(monitor1.statusSnapshots.poll(), taskFut.getTaskSession());
        }
        finally {
            CRD.context().task().stopListenStatusUpdates(monitor1);
        }

        taskFut.get(getTestTimeout());
    }

    /** */
    private void checkSnapshot(ComputeTaskStatusSnapshot snapshot, ComputeTaskSession session) {
        assertEquals(RUNNING, snapshot.status());

        assertEquals(session.getId(), snapshot.sessionId());
        assertEquals(session.getTaskName(), snapshot.taskName());
        assertEquals(session.getTaskNodeId(), snapshot.originatingNodeId());
        assertEquals(session.getStartTime(), snapshot.startTime());
        assertEquals(session.getEndTime(), snapshot.endTime());

        assertTrue(snapshot.attributes().isEmpty());
        assertNull(snapshot.failReason());

        assertEqualsCollections(
            new TreeSet<>(session.getTopology()),
            new TreeSet<>(snapshot.jobNodes())
        );
    }

    /** */
    private void checkFailTask(ComputeTaskStatusDiff diff, ComputeTaskSession session) {
        assertTrue(diff.taskFinished());

        assertFalse(diff.newTask());
        assertFalse(diff.hasJobNodesChanged());
        assertFalse(diff.hasAttributesChanged());

        assertEquals(session.getId(), diff.sessionId());
        assertEquals(FAILED, diff.status());
        assertNotNull(diff.failReason());

        assertNull(diff.taskName());
        assertNull(diff.originatingNodeId());

        assertEquals(0L, diff.startTime());
        assertEquals(0L, diff.endTime());

        assertTrue(diff.jobNodes().isEmpty());
        assertTrue(diff.attributes().isEmpty());
    }

    /** */
    private void checkFinishTask(ComputeTaskStatusDiff diff, ComputeTaskSession session) {
        assertTrue(diff.taskFinished());

        assertFalse(diff.newTask());
        assertFalse(diff.hasJobNodesChanged());
        assertFalse(diff.hasAttributesChanged());

        assertEquals(session.getId(), diff.sessionId());
        assertEquals(FINISHED, diff.status());

        assertNull(diff.taskName());
        assertNull(diff.originatingNodeId());
        assertNull(diff.failReason());

        assertEquals(0L, diff.startTime());
        assertEquals(0L, diff.endTime());

        assertTrue(diff.jobNodes().isEmpty());
        assertTrue(diff.attributes().isEmpty());
    }

    /** */
    private void checkChangeJobNodes(ComputeTaskStatusDiff diff, ComputeTaskSession session) {
        assertTrue(diff.hasJobNodesChanged());

        assertFalse(diff.newTask());
        assertFalse(diff.hasAttributesChanged());
        assertFalse(diff.taskFinished());

        assertEquals(session.getId(), diff.sessionId());

        assertEqualsCollections(
            new TreeSet<>(session.getTopology()),
            new TreeSet<>(diff.jobNodes())
        );

        assertNull(diff.status());
        assertNull(diff.taskName());
        assertNull(diff.originatingNodeId());
        assertNull(diff.failReason());

        assertEquals(0L, diff.startTime());
        assertEquals(0L, diff.endTime());

        assertTrue(diff.attributes().isEmpty());
    }

    /** */
    private void checkChangeAttributesNodes(ComputeTaskStatusDiff diff, ComputeTaskSession session) {
        assertTrue(diff.hasAttributesChanged());

        assertFalse(diff.newTask());
        assertFalse(diff.hasJobNodesChanged());
        assertFalse(diff.taskFinished());

        assertEquals(session.getId(), diff.sessionId());

        assertEquals(
            new TreeMap<>(session.getAttributes()),
            new TreeMap<>(diff.attributes())
        );

        assertNull(diff.status());
        assertNull(diff.taskName());
        assertNull(diff.originatingNodeId());
        assertNull(diff.failReason());

        assertEquals(0L, diff.startTime());
        assertEquals(0L, diff.endTime());

        assertTrue(diff.jobNodes().isEmpty());
    }

    /** */
    private void checkStartTask(ComputeTaskStatusDiff diff, ComputeTaskSession session) {
        assertTrue(diff.newTask());

        assertFalse(diff.hasJobNodesChanged());
        assertFalse(diff.hasAttributesChanged());
        assertFalse(diff.taskFinished());

        assertEquals(session.getId(), diff.sessionId());
        assertEquals(RUNNING, diff.status());

        assertEquals(session.getTaskName(), diff.taskName());
        assertEquals(session.getTaskNodeId(), diff.originatingNodeId());
        assertEquals(session.getStartTime(), diff.startTime());
        assertEquals(session.getEndTime(), diff.endTime());

        assertTrue(diff.jobNodes().isEmpty());
        assertTrue(diff.attributes().isEmpty());

        assertNull(diff.failReason());
    }

    /** */
    private static class ComputeGridMonitorImpl implements ComputeGridMonitor {
        /** */
        final Queue<ComputeTaskStatusSnapshot> statusSnapshots = new ConcurrentLinkedQueue<>();

        /** */
        final Queue<ComputeTaskStatusDiff> statusDiffs = new ConcurrentLinkedQueue<>();

        /** {@inheritDoc} */
        @Override public void processStatusSnapshots(Collection<ComputeTaskStatusSnapshot> snapshots) {
            statusSnapshots.addAll(snapshots);
        }

        /** {@inheritDoc} */
        @Override public void processStatusDiff(ComputeTaskStatusDiff diff) {
            statusDiffs.add(diff);
        }
    }

    /** */
    private static class NoopComputeTask extends ComputeTaskAdapter<Void, Void> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            Void arg
        ) throws IgniteException {
            return subgrid.stream().collect(toMap(n -> new NoopComputeJob(), identity()));
        }

        /** {@inheritDoc} */
        @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }

    /** */
    @ComputeTaskSessionFullSupport
    private static class ComputeFullWithWaitTask extends ComputeTaskAdapter<Void, Void> {
        /** */
        final GridFutureAdapter<Void> doneOnMapFut = new GridFutureAdapter<>();

        /** */
        final GridFutureAdapter<Void> waitOnJobExecFut = new GridFutureAdapter<>();

        /** */
        final long timeout;

        /** */
        public ComputeFullWithWaitTask(long timeout) {
            this.timeout = timeout;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            Void arg
        ) throws IgniteException {
            doneOnMapFut.onDone();

            return subgrid.stream().collect(toMap(n -> new NoopComputeJob() {
                /** {@inheritDoc} */
                @Override public Object execute() throws IgniteException {
                    try {
                        U.sleep(500);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        throw new IgniteException(e);
                    }

                    return super.execute();
                }
            }, identity()));
        }

        /** {@inheritDoc} */
        @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }

    /** */
    private static class NoopComputeJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            return null;
        }
    }
}

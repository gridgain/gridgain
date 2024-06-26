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

package org.apache.ignite.session;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Session cancellation tests.
 */
@GridCommonTest(group = "Task Session")
public class GridSessionCancelSiblingsFromTaskSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int WAIT_TIME = 20000;

    /** */
    public static final int SPLIT_COUNT = 5;

    /** */
    public static final int EXEC_COUNT = 5;

    /** */
    private static AtomicInteger[] interruptCnt;

    /** */
    private static CountDownLatch[] startSignal;

    /** */
    private static CountDownLatch[] stopSignal;

    /** */
    public GridSessionCancelSiblingsFromTaskSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(discoSpi);

        c.setPublicThreadPoolSize(SPLIT_COUNT * EXEC_COUNT);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeFirstTest() throws Exception {
        super.beforeFirstTest();

        // We are changing it because compute jobs fall asleep.
        assertTrue(computeJobWorkerInterruptTimeout(G.ignite(getTestIgniteInstanceName())).propagate(10L));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCancelSiblings() throws Exception {
        refreshInitialData();

        for (int i = 0; i < EXEC_COUNT; i++)
            checkTask(i);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultiThreaded() throws Exception {
        refreshInitialData();

        final GridThreadSerialNumber sNum = new GridThreadSerialNumber();

        final AtomicBoolean failed = new AtomicBoolean(false);

        GridTestUtils.runMultiThreaded(() -> {
            int num = sNum.get();

            try {
                checkTask(num);
            }
            catch (Throwable e) {
                error("Failed to execute task.", e);

                failed.set(true);
            }
        }, EXEC_COUNT, "grid-session-test");

        if (failed.get())
            fail();
    }

    /**
     * @param num Task number.
     * @throws InterruptedException If interrupted.
     * @throws IgniteCheckedException If failed.
     */
    private void checkTask(int num) throws InterruptedException, IgniteCheckedException {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

        ComputeTaskFuture<?> fut = executeAsync(ignite.compute(), GridTaskSessionTestTask.class, num);

        assertNotNull(fut);

        try {
            // Wait until jobs begin execution.
            assertTrue("Jobs did not start.", startSignal[num].await(WAIT_TIME, TimeUnit.MILLISECONDS));

            Object res = fut.get(getTestTimeout());

            assertThat(res, equalTo("interrupt-task-data"));

            // Wait for all jobs to finish.
            assertTrue(
                "Jobs did not cancel [interruptCount=" + Arrays.toString(interruptCnt) + ']',
                stopSignal[num].await(WAIT_TIME, TimeUnit.MILLISECONDS)
            );

            assertThat(interruptCnt[num].get(), equalTo(SPLIT_COUNT - 1));
        }
        finally {
            // We must wait for the jobs to be sure that they have completed
            // their execution since they use static variable (shared for the tests).
            fut.get(getTestTimeout());
        }
    }

    /** */
    private void refreshInitialData() {
        interruptCnt = new AtomicInteger[EXEC_COUNT];
        startSignal = new CountDownLatch[EXEC_COUNT];
        stopSignal = new CountDownLatch[EXEC_COUNT];

        for (int i = 0; i < EXEC_COUNT; i++) {
            interruptCnt[i] = new AtomicInteger(0);

            startSignal[i] = new CountDownLatch(SPLIT_COUNT);

            stopSignal[i] = new CountDownLatch(SPLIT_COUNT - 1);
        }
    }

    /**
     *
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class GridTaskSessionTestTask extends ComputeTaskSplitAdapter<Serializable, String> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        @TaskSessionResource
        private ComputeTaskSession taskSes;

        /** */
        private volatile int taskNum = -1;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Serializable arg) {
            if (log.isInfoEnabled())
                log.info("Splitting job [job=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            assertNotNull(arg);

            taskNum = (Integer)arg;

            assertThat(taskNum, not(equalTo(-1)));

            Collection<ComputeJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 1; i <= SPLIT_COUNT; i++) {
                jobs.add(new ComputeJobAdapter(i) {
                    /** */
                    private volatile Thread thread;

                    /** {@inheritDoc} */
                    @Override public Serializable execute() {
                        assertNotNull(taskSes);

                        thread = Thread.currentThread();

                        int arg = this.<Integer>argument(0);

                        if (log.isInfoEnabled())
                            log.info("Computing job [job=" + this + ", arg=" + arg + ']');

                        startSignal[taskNum].countDown();

                        try {
                            if (!startSignal[taskNum].await(WAIT_TIME, TimeUnit.MILLISECONDS))
                                fail();

                            if (arg == 1) {
                                if (log.isInfoEnabled())
                                    log.info("Job one is proceeding.");
                            }
                            else
                                Thread.sleep(WAIT_TIME);
                        }
                        catch (InterruptedException e) {
                            if (log.isInfoEnabled())
                                log.info("Job got interrupted [arg=" + arg + ", e=" + e + ']');

                            return "interrupt-job-data";
                        }

                        if (log.isInfoEnabled())
                            log.info("Completing job: " + taskSes);

                        return arg;
                    }

                    /** {@inheritDoc} */
                    @Override public void cancel() {
                        assertNotNull(thread);

                        interruptCnt[taskNum].incrementAndGet();

                        stopSignal[taskNum].countDown();
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult result, List<ComputeJobResult> received) {
            if (received.size() == 1) {
                Collection<ComputeJobSibling> jobSiblings = taskSes.getJobSiblings();

                IgniteUuid jobId = received.get(0).getJobContext().getJobId();

                assertNotNull(jobId);

                // Cancel all jobs except first job with argument 1.
                for (ComputeJobSibling jobSibling : jobSiblings) {
                    if (!jobId.equals(jobSibling.getJobId()))
                        jobSibling.cancel();
                }
            }

            return received.size() == SPLIT_COUNT ? ComputeJobResultPolicy.REDUCE : ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public String reduce(List<ComputeJobResult> results) {
            if (log.isInfoEnabled())
                log.info("Aggregating job [job=" + this + ", results=" + results + ']');

            if (results.size() != SPLIT_COUNT)
                fail("Invalid results size.");

            return "interrupt-task-data";
        }
    }
}

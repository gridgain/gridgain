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
 * Test of session siblings cancellation from future.
 */
@GridCommonTest(group = "Task Session")
public class GridSessionCancelSiblingsFromFutureSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int WAIT_TIME = 20000;

    /** */
    public static final int SPLIT_COUNT = 5;

    /** */
    public static final int EXEC_COUNT = 5;

    /** */
    private static AtomicInteger[] interruptCnt;

    /** */
    private static CountDownLatch[] startSig;

    /** */
    private static CountDownLatch[] stopSig;

    /**
     *
     */
    public GridSessionCancelSiblingsFromFutureSelfTest() throws Exception {
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
     * @throws Exception if failed
     */
    @Test
    public void testCancelSiblings() throws Exception {
        refreshInitialData();

        for (int i = 0; i < EXEC_COUNT; i++)
            checkTask(i);
    }

    /**
     * @throws Exception if failed
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
     * @param num Task argument.
     * @throws InterruptedException if failed
     * @throws IgniteCheckedException if failed
     */
    private void checkTask(int num) throws InterruptedException, IgniteCheckedException {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

        ComputeTaskFuture<?> fut = executeAsync(ignite.compute(), GridTaskSessionTestTask.class, num);

        assertNotNull(fut);

        try {
            // Wait until jobs begin execution.
            assertTrue("Jobs did not start.", startSig[num].await(WAIT_TIME, TimeUnit.MILLISECONDS));

            Collection<ComputeJobSibling> jobSiblings = fut.getTaskSession().getJobSiblings();

            // Cancel all jobs.
            for (ComputeJobSibling jobSibling : jobSiblings) {
                jobSibling.cancel();
            }

            Object res = fut.get(getTestTimeout());

            assertThat(res, equalTo("interrupt-task-data"));

            // Wait for all jobs to finish.
            assertTrue("Jobs did not cancel.", stopSig[num].await(WAIT_TIME, TimeUnit.MILLISECONDS));

            assertThat(interruptCnt[num].get(), equalTo(SPLIT_COUNT));
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
        startSig = new CountDownLatch[EXEC_COUNT];
        stopSig = new CountDownLatch[EXEC_COUNT];

        for (int i = 0; i < EXEC_COUNT; i++) {
            interruptCnt[i] = new AtomicInteger(0);

            startSig[i] = new CountDownLatch(SPLIT_COUNT);

            stopSig[i] = new CountDownLatch(SPLIT_COUNT);
        }
    }

    /**
     *
     */
    private static class GridTaskSessionTestTask extends ComputeTaskSplitAdapter<Serializable, String> {
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
                log.info("Splitting jobs [task=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            assertNotNull(arg);

            taskNum = (Integer)arg;

            assertThat(taskNum, not(equalTo(-1)));

            Collection<ComputeJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 1; i <= SPLIT_COUNT; i++) {
                jobs.add(new ComputeJobAdapter(i) {
                    private volatile Thread thread;

                    @Override public Serializable execute() {
                        assertNotNull(taskSes);

                        thread = Thread.currentThread();

                        if (log.isInfoEnabled())
                            log.info("Computing job [job=" + this + ", arg=" + argument(0) + ']');

                        startSig[taskNum].countDown();

                        try {
                            if (!startSig[taskNum].await(WAIT_TIME, TimeUnit.MILLISECONDS))
                                fail();

                            Thread.sleep(WAIT_TIME);
                        }
                        catch (InterruptedException e) {
                            if (log.isInfoEnabled())
                                log.info("Job got interrupted [arg=" + argument(0) + ", e=" + e + ']');

                            return "interrupt-job-data";
                        }

                        if (log.isInfoEnabled())
                            log.info("Completing job: " + taskSes);

                        return argument(0);
                    }

                    @Override public void cancel() {
                        assertNotNull(thread);

                        interruptCnt[taskNum].incrementAndGet();

                        stopSig[taskNum].countDown();
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            if (log.isInfoEnabled())
                log.info("Received job result [job=" + this + ", result=" + res + ']');

            return rcvd.size() == SPLIT_COUNT ? ComputeJobResultPolicy.REDUCE : ComputeJobResultPolicy.WAIT;
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

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

package org.apache.ignite.session;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.events.ComputeTaskEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.events.EventType.EVTS_TASK_EXECUTION;
import static org.apache.ignite.events.EventType.EVT_TASK_SESSION_ATTR_SET;
import static org.apache.ignite.events.EventType.EVT_TASK_STARTED;

/**
 *
 */
@GridCommonTest(group = "Task Session")
public class GridSessionSetTaskAttributeSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int SPLIT_COUNT = 5;

    /** */
    public static final int EXEC_COUNT = 5;

    /** */
    private static final String INT_PARAM_NAME = "customIntParameter";

    /** */
    private static final String TXT_PARAM_NAME = "customTextParameter";

    /** */
    public GridSessionSetTaskAttributeSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
                .setIncludeEventTypes(EVTS_TASK_EXECUTION);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testSetAttribute() throws Exception {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

        ignite.compute().localDeployTask(GridTaskSessionTestTask.class, GridTaskSessionTestTask.class.getClassLoader());

        for (int i = 0; i < EXEC_COUNT; i++)
            checkTask(i);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testMultiThreaded() throws Exception {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

        ignite.compute().localDeployTask(GridTaskSessionTestTask.class, GridTaskSessionTestTask.class.getClassLoader());

        final GridThreadSerialNumber sNum = new GridThreadSerialNumber();

        final AtomicBoolean failed = new AtomicBoolean(false);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                int num = sNum.get();

                try {
                    checkTask(num);
                }
                catch (Throwable e) {
                    error("Failed to execute task.", e);

                    failed.set(true);
                }
            }
        }, EXEC_COUNT, "grid-session-test");

        if (failed.get())
            fail();
    }

    /**
     * Check parameters propagation from session to events.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAttributesToEventsPropagation() throws Exception {
        int intParam = 1;
        String textParam = "text";

        IgniteEx n = startGrid(0);

        n.cluster().state(ACTIVE);

        GridFutureAdapter<Void> lsnrFut = new CountDownFuture(5);

        IgnitePredicate<TaskEvent> lsnr = evt -> {
            assertTrue(evt instanceof ComputeTaskEvent);

            ComputeTaskEvent event = (ComputeTaskEvent) evt;

            log.info("Received task event [evt=" + event.name() + ", taskName=" + event.taskName() +
                ", taskAttributes=" + event.attributes() + ']');

            try {
                if (event.type() == EVT_TASK_STARTED)
                    assertTrue(event.attributes().isEmpty());
                else if (event.type() == EVT_TASK_SESSION_ATTR_SET) {
                    if (event.attributes().containsKey(INT_PARAM_NAME))
                        assertEquals(intParam, event.attributes().get(INT_PARAM_NAME));

                    if (event.attributes().containsKey(TXT_PARAM_NAME))
                        assertEquals(textParam, event.attributes().get(TXT_PARAM_NAME));
                }
                else {
                    assertEquals(intParam, event.attributes().get(INT_PARAM_NAME));
                    assertEquals(textParam, event.attributes().get(TXT_PARAM_NAME));
                }

                lsnrFut.onDone();
            }
            catch (Throwable t) {
                lsnrFut.onDone(t);
            }
            finally {
                return true;
            }
        };

        n.events().localListen(lsnr, EVTS_TASK_EXECUTION);

        // Generate task events.
        IgniteFuture<Void> taskFut = n.compute().runAsync(new RunnableWithSessionAttributes(intParam, textParam));

        taskFut.get(5, TimeUnit.SECONDS);

        lsnrFut.get(5, TimeUnit.SECONDS);

        // Unsubscribe local task event listener.
        n.events().stopLocalListen(lsnr);
    }

    /**
     * @param num Number.
     */
    private void checkTask(int num) {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

        ComputeTaskFuture<?> fut = ignite.compute().executeAsync(GridTaskSessionTestTask.class.getName(), num);

        Object res = fut.get();

        assert (Integer)res == SPLIT_COUNT : "Invalid result [num=" + num + ", fut=" + fut + ']';
    }

    /**
     *
     */
    @ComputeTaskSessionFullSupport
    private static class RunnableWithSessionAttributes implements IgniteRunnable {
        @TaskSessionResource
        private ComputeTaskSession ses;

        private int numericParameter;

        private String textParameter;

        public RunnableWithSessionAttributes(int numericParameter, String textParameter) {
            this.numericParameter = numericParameter;
            this.textParameter = textParameter;
        }

        @Override public void run() {
            ses.setAttribute(INT_PARAM_NAME, numericParameter);
            ses.setAttribute(TXT_PARAM_NAME, textParameter);
        }
    }

    /**
     *
     */
    @ComputeTaskSessionFullSupport
    private static class GridTaskSessionTestTask extends ComputeTaskSplitAdapter<Serializable, Integer> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        @TaskSessionResource
        private ComputeTaskSession taskSes;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Serializable arg) {
            assert taskSes != null;

            if (log.isInfoEnabled())
                log.info("Splitting job [job=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            Collection<ComputeJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 1; i <= SPLIT_COUNT; i++) {
                jobs.add(new ComputeJobAdapter(i) {
                    @Override public Serializable execute() {
                        assert taskSes != null;

                        if (log.isInfoEnabled())
                            log.info("Computing job [job=" + this + ", arg=" + argument(0) + ']');

                        try {
                            String val = (String)taskSes.waitForAttribute("testName", 20000);

                            if (log.isInfoEnabled())
                                log.info("Received attribute 'testName': " + val);

                            if ("testVal".equals(val))
                                return 1;
                        }
                        catch (InterruptedException e) {
                            throw new IgniteException("Failed to get attribute due to interruption.", e);
                        }

                        return 0;
                    }
                });
            }

            if (log.isInfoEnabled())
                log.info("Set attribute 'testName'.");

            taskSes.setAttribute("testName", "testVal");

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received) {
            if (res.getException() != null)
                throw res.getException();

            if (log.isInfoEnabled()) {
                log.info("Received result from job [res=" + res + ", size=" + received.size() +
                    ", received=" + received + ']');
            }

            return received.size() == SPLIT_COUNT ? ComputeJobResultPolicy.REDUCE : ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
            if (log.isInfoEnabled())
                log.info("Reducing job [job=" + this + ", results=" + results + ']');

            if (results.size() < SPLIT_COUNT)
                fail();

            int sum = 0;

            for (ComputeJobResult result : results) {
                if (result.getData() != null)
                    sum += (Integer)result.getData();
            }

            return sum;
        }
    }
}

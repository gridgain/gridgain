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
package org.apache.ignite.internal.compute.flow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.compute.ComputeJobResultPolicy.WAIT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

public class TaskFlowTest extends GridCommonAbstractTest {
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName);
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    @Test
    public void test() throws Exception {
        int nodeCnt = 3;

        IgniteEx ignite = startGrids(nodeCnt);

        // Build a flow.
        TaskFlow flow = new TaskFlow(
            FlowElement.builder("str", new StringHashTaskAdapter())
                .add(FlowElement.builder("int", new SqrIntTaskAdapter()).build())
                .build()
        );

        // Save it.
        ignite.context().flowProcessor().addFlow("asd", flow, false);

        // Execute.
        IgniteFuture<FlowTaskTransferObject> fut = ignite.context().flowProcessor()
            .executeFlow("asd", new FlowTaskTransferObject("string", "zxc_"));

        // Execution result is wrapped in FlowTaskTransferObject.
        FlowTaskTransferObject res = fut.get();

        int r = (int)res.data().get("int");

        int expected = expectedRes(nodeCnt);

        assertTrue(res.successfull());

        assertEquals(expected, r);
    }

    @Test
    public void testFailingTask() throws Exception {
        int nodeCnt = 3;

        IgniteEx ignite = startGrids(nodeCnt);

        TaskFlow flow = new TaskFlow(
            FlowElement.builder("str", new StringHashTaskAdapter())
                .add(FlowElement.builder("int", new SqrIntTaskAdapter()).build())
                .build()
        );

        ignite.context().flowProcessor().addFlow("asd", flow, false);

        IgniteFuture<FlowTaskTransferObject> fut = ignite.context().flowProcessor()
            .executeFlow("asd", new FlowTaskTransferObject("string", "qwe_"));

        assertThrows(log, () -> fut.get(), IgniteException.class, "qwe not allowed");
    }

    @Test
    public void testFailingTaskWithExceptionCatcher() throws Exception {
        int nodeCnt = 3;

        IgniteEx ignite = startGrids(nodeCnt);

        TaskFlow flow = new TaskFlow(
            FlowElement.builder("str", new StringHashTaskAdapter())
                .add(FlowElement.builder("int", new SqrIntTaskAdapter()).build())
                .exceptionally(FlowElement.builder("exc", new OnExceptionTaskAdapter()).build())
                .build()
        );

        ignite.context().flowProcessor().addFlow("asd", flow, false);

        IgniteFuture<FlowTaskTransferObject> fut = ignite.context().flowProcessor()
            .executeFlow("asd", new FlowTaskTransferObject("string", "qwe_"));

        FlowTaskTransferObject res = fut.get();

        String r = (String)res.data().get("str");

        assertTrue(res.successfull());

        assertEquals("qwe not allowed", r);
    }

    private int expectedRes(int nodeCnt) {
        int a = 0;

        for (int i = 0; i < nodeCnt; i++)
            a += ("zxc_" + grid(i).cluster().localNode().id().toString()).hashCode();

        return a * a;
    }

    private static class StringHashTask implements ComputeTask<String, Integer> {
        AtomicInteger hashSum = new AtomicInteger(0);
        IgniteException ex;

        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) throws IgniteException {
            return subgrid.stream().collect(toMap(n -> new StringHashJob(arg + n.id().toString()), n -> n));
        }

        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
            log.info("zzz adding " + res.getData());

            if (res.getData() != null)
                hashSum.addAndGet(res.getData());
            else
                ex = res.getException();

            return WAIT;
        }

        @Override public Integer reduce(List<ComputeJobResult> list) throws IgniteException {
            if (ex != null)
                throw ex;

            return hashSum.get();
        }
    }

    private static class StringHashJob implements ComputeJob {
        String arg;

        StringHashJob(String arg) {
            this.arg = arg;
        }

        @Override public void cancel() {

        }

        @Override public Object execute() throws IgniteException {
            log.info("zzz calculated from " + arg + " value " + arg.hashCode());

            if (arg.startsWith("qwe"))
                throw new IgniteException("qwe not allowed");

            return arg.hashCode();
        }
    }

    private static class StringHashTaskAdapter implements ComputeTaskFlowAdapter<StringHashTask, String, Integer> {
        @Override public Class<StringHashTask> taskClass() {
            return StringHashTask.class;
        }

        @Override public String parameters(FlowTaskTransferObject input) {
            return (String)input.data().get("string");
        }

        @Override public FlowTaskTransferObject result(Integer integer) {
            return new FlowTaskTransferObject("int", integer);
        }

        @Override public IgnitePredicate<ClusterNode> nodeFilter() {
            return null;
        }
    }

    private static class SqrIntTask implements ComputeTask<Integer, Integer> {
        private volatile int res;

        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Integer arg) throws IgniteException {
            Map<SqrIntJob, ClusterNode> res = new HashMap<>();

            log.info("zzz sqr arg: " + arg);

            res.put(new SqrIntJob(arg), subgrid.get(0));

            return res;
        }

        @Override
        public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
            this.res = res.getData();

            return WAIT;
        }

        @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteException {
            return res;
        }
    }

    private static class SqrIntJob implements ComputeJob {
        Integer arg;

        public SqrIntJob(Integer arg) {
            this.arg = arg;
        }

        @Override public void cancel() {

        }

        @Override public Integer execute() throws IgniteException {
            return arg * arg;
        }
    }

    private static class SqrIntTaskAdapter implements ComputeTaskFlowAdapter<SqrIntTask, Integer, Integer> {
        @Override public Class<SqrIntTask> taskClass() {
            return SqrIntTask.class;
        }

        @Override public Integer parameters(FlowTaskTransferObject input) {
            return (Integer)input.data().get("int");
        }

        @Override public FlowTaskTransferObject result(Integer r) {
            return new FlowTaskTransferObject("int", r);
        }

        @Override public IgnitePredicate<ClusterNode> nodeFilter() {
            return null;
        }
    }

    private static class OnExceptionTask implements ComputeTask<Throwable, String> {
        String res;
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Throwable arg) throws IgniteException {
            Map<OnExceptionJob, ClusterNode> res = new HashMap<>();

            res.put(new OnExceptionJob(arg), subgrid.get(0));

            return res;
        }

        @Override
        public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
            this.res = res.getData();

            return WAIT;
        }

        @Override public String reduce(List<ComputeJobResult> results) throws IgniteException {
            return res;
        }
    }

    private static class OnExceptionJob implements ComputeJob {
        Throwable e;

        public OnExceptionJob(Throwable e) {
            this.e = e;
        }

        @Override public void cancel() {

        }

        @Override public String execute() throws IgniteException {
            return e.getMessage();
        }
    }

    private static class OnExceptionTaskAdapter implements ComputeTaskFlowAdapter<OnExceptionTask, Throwable, String> {
        @Override public Class<OnExceptionTask> taskClass() {
            return OnExceptionTask.class;
        }

        @Override public Throwable parameters(FlowTaskTransferObject input) {
            return input.exception();
        }

        @Override public FlowTaskTransferObject result(String r) {
            return new FlowTaskTransferObject("str", r);
        }

        @Override public IgnitePredicate<ClusterNode> nodeFilter() {
            return null;
        }
    }
}

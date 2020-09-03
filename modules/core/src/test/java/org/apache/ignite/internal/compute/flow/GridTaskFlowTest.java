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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.compute.ComputeJobResultPolicy.WAIT;

public class GridTaskFlowTest extends GridCommonAbstractTest {
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName);
    }

    @Test
    public void test() throws Exception {
        IgniteEx ignite = startGrids(3);

        GridTaskFlow flow = new GridTaskFlowBuilder(new AnyResultAggregator())
            .addTask()
            .build();

        ignite.context().flowProcessor().addFlow("asd", flow, false);

        IgniteFuture<GridFlowTaskTransferObject> fut = ignite.context().flowProcessor().executeFlow("asd");

        GridFlowTaskTransferObject res = fut.get();

        assertTrue(res.successfull());
    }

    private static class StringHashTask implements FlowTask<String, Integer> {
        AtomicInteger hashSum = new AtomicInteger(0);

        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) throws IgniteException {
            return subgrid.stream().collect(toMap(n -> new StringHashJob(arg), n -> n));
        }

        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
            hashSum.addAndGet(res.getData());

            return WAIT;
        }

        @Override public Integer reduce(List<ComputeJobResult> list) throws IgniteException {
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
            return arg.hashCode();
        }
    }

    private static class StringHashTaskAdapter implements GridFlowTaskAdapter<StringHashTask, String, Integer> {
        @Override public Class<StringHashTask> taskClass() {
            return StringHashTask.class;
        }

        @Override public String arguments(GridFlowTaskTransferObject parentResult) {
            return (String)parentResult.data().get("string");
        }

        @Override public GridFlowTaskTransferObject result(Integer integer) {
            return new GridFlowTaskTransferObject(new IgniteBiTuple<>("int", integer));
        }

        @Override public IgnitePredicate<ClusterNode> nodeFilter() {
            return null;
        }
    }

    private static class SqrIntTask implements FlowTask<Integer, Integer> {

        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Integer arg) throws IgniteException {
            Map<SqrIntJob, ClusterNode> res = new HashMap<>();

            res.put(new SqrIntJob(arg), subgrid.get(0));

            return res;
        }

        @Override
        public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
            return WAIT;
        }

        @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
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
}
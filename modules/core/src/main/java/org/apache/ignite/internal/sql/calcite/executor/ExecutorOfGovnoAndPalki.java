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
package org.apache.ignite.internal.sql.calcite.executor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.sql.calcite.plan.OutputNode;
import org.apache.ignite.internal.sql.calcite.plan.PlanStep;
import org.apache.ignite.internal.sql.calcite.plan.SenderNode;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteFuture;

import static org.apache.ignite.internal.sql.calcite.plan.PlanStep.Distribution.ALL_NODES;

/**
 * Made in the worst tradition. Do not use it in production code.
 */
public class ExecutorOfGovnoAndPalki implements GridMessageListener {

    private final AtomicLong qryIdCntr = new AtomicLong();

    private GridKernalContext ctx;

    private Map<T2<UUID, Long>, Map<Integer, PlanStep>> runningQrys = new HashMap<>();

    /** */
    public ExecutorOfGovnoAndPalki(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** */
    public List<FieldsQueryCursor<List<?>>> execute(List<PlanStep> multiStepPlan) {
        FieldsQueryCursor<List<?>> cursor = submitQuery(multiStepPlan).get();

        return Collections.singletonList(cursor);
    }

    /** */
    private IgniteFuture<FieldsQueryCursor<List<?>>> submitQuery(List<PlanStep> multiStepPlan) {
        assert multiStepPlan.get(multiStepPlan.size()).plan() instanceof OutputNode; // Last node is expected to be the output node.

        UUID locNode = ctx.localNodeId();
        long cntr = qryIdCntr.getAndIncrement();

        T2<UUID, Long> qryId = new T2<>(locNode, cntr);

        Map<Integer, PlanStep> locNodePlan = new HashMap<>(); // Plan for the local node.
        List<PlanStep> globalPlan = new ArrayList<>(); // Plan for other data nodes.

        for (PlanStep step : multiStepPlan) {
            locNodePlan.put(step.id(), step);

            if (step.distribution() == ALL_NODES)
                globalPlan.add(step);
        }

        runningQrys.put(qryId, locNodePlan);

        return new IgniteFinishedFutureImpl<>(new QueryCursorImpl<>(Collections.emptyList()));
    }

    /** */
    public void sendResult(List<List<?>> result, SenderNode.SenderType type, int linkId, T2<UUID, Long> qryId) {

    }

    /** */
    @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
        // TODO: CODE: implement.
    }

}

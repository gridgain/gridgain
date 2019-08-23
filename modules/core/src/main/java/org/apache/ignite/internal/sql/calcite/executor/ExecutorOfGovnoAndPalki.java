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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.sql.calcite.plan.OutputNode;
import org.apache.ignite.internal.sql.calcite.plan.PlanStep;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;

import static org.apache.ignite.internal.sql.calcite.plan.PlanStep.Site.ALL_NODES;

/**
 * Made in the worst tradition. Do not use it in production code.
 */
public class ExecutorOfGovnoAndPalki implements GridMessageListener {

    private final AtomicLong qryIdCntr = new AtomicLong();

    private GridKernalContext ctx;

    private Marshaller marshaller;

    private Map<T2<UUID, Long>, Map<Integer, PlanStep>> runningQrys = new HashMap<>();

    private List<GridCacheContext> caches = new ArrayList<>();

    private Map<T2<UUID, Long>, IgniteInternalFuture<FieldsQueryCursor<List<?>>>> futs = new HashMap<>();

    /**
     *
     */
    public ExecutorOfGovnoAndPalki(GridKernalContext ctx) {
        this.ctx = ctx;

        marshaller = ctx.config().getMarshaller();
    }

    /**
     *
     */
    public List<FieldsQueryCursor<List<?>>> execute(List<PlanStep> multiStepPlan) {
        try {
            FieldsQueryCursor<List<?>> cursor = submitQuery(multiStepPlan).get();

            return Collections.singletonList(cursor);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     *
     */
    private IgniteInternalFuture<FieldsQueryCursor<List<?>>> submitQuery(List<PlanStep> multiStepPlan) {
        assert multiStepPlan.get(multiStepPlan.size() - 1).plan() instanceof OutputNode; // Last node is expected to be the output node.

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

        IgniteInternalFuture<FieldsQueryCursor<List<?>>> qryFut = new GridFutureAdapter<>();

        synchronized (this) {
            runningQrys.put(qryId, locNodePlan);

            futs.put(qryId, qryFut);
        }

        sendRequests(qryId, globalPlan);

        return qryFut;
    }

    private void sendRequests(T2<UUID, Long> qryId, List<PlanStep> plan) {
        try {
            QueryRequest req = new QueryRequest(qryId, plan);

            for (ClusterNode node : ctx.discovery().remoteNodes()) {

                if (!node.isClient()) {
                    send(req, node);
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     *
     */
    public void sendResult(List<List<?>> result, int linkId, T2<UUID, Long> qryId, UUID dest) {

    }

    private void send(Message msg, ClusterNode node) throws IgniteCheckedException {
        if (msg instanceof GridCacheQueryMarshallable)
            ((GridCacheQueryMarshallable)msg).marshall(marshaller);

        ctx.io().sendToGridTopic(node, GridTopic.TOPIC_QUERY, msg, GridIoPolicy.QUERY_POOL);
    }

    /**
     *
     */
    @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
        if (msg instanceof GridCacheQueryMarshallable)
            ((GridCacheQueryMarshallable)msg).unmarshall(marshaller, ctx);

        System.out.println("onMessage node =" + nodeId + ", msg= " + msg);
    }

    public void onCacheRegistered(GridCacheContext cache) {
        if (cache.userCache())
            caches.add(cache);
    }

    public GridCacheContext firstUserCache() {
        return caches.get(0);
    }

}

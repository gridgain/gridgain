/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.checker;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.processors.cache.FinalizeCountersDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.verify.ReconciliationType;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.JobContextResource;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 * Partition reconciliation job.
 * @param <ResultT> Result type.
 */
public class VisorPartitionReconciliationJob<ResultT> extends VisorJob<VisorPartitionReconciliationTaskArg, ResultT> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteFuture<ResultT> fut;

    /** Auto-inject job context. */
    @JobContextResource
    protected transient ComputeJobContext jobCtx;

    /** Task class for execution */
    private final Class<? extends ComputeTask<VisorPartitionReconciliationTaskArg, ResultT>> taskCls;

    /**
     * @param arg Argument.
     * @param debug Debug.
     * @param taskCls Task class for execution.
     */
    VisorPartitionReconciliationJob(
        VisorPartitionReconciliationTaskArg arg,
        boolean debug,
        Class<? extends ComputeTask<VisorPartitionReconciliationTaskArg, ResultT>> taskCls
    ) {
        super(arg, debug);
        this.taskCls = taskCls;
    }

    /** {@inheritDoc} */
    @Override protected ResultT run(@Nullable VisorPartitionReconciliationTaskArg arg) throws IgniteException {
        if (fut == null) {
            try {
                if (arg.reconTypes().contains(ReconciliationType.PARTITION_COUNTER_CONSISTENCY) &&
                    arg.repair() &&
                    IgniteFeatures.allNodesSupports(
                        ignite.context(),
                        ignite.context().discovery().allNodes(),
                        IgniteFeatures.PARTITION_RECONCILIATION_V2)) {
                    FinalizeCountersDiscoveryMessage msg = new FinalizeCountersDiscoveryMessage();

                    GridFutureAdapter<Void> partCntFut = new GridFutureAdapter<>();
                    GridFutureAdapter<ResultT> resFut = new GridFutureAdapter<>();

                    GridCachePartitionExchangeManager exchMgr = ignite
                        .context()
                        .cache()
                        .context()
                        .exchange();

                    exchMgr.registerExchangeAwareComponent(new PartitionsExchangeAware() {
                        /** {@inheritDoc} */
                        @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
                            DiscoveryEvent evt = fut.firstEvent();

                            if (evt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                                DiscoveryCustomEvent customEvt = (DiscoveryCustomEvent)evt;

                                if (customEvt.customMessage() instanceof FinalizeCountersDiscoveryMessage) {
                                    FinalizeCountersDiscoveryMessage discoMsg = (FinalizeCountersDiscoveryMessage)customEvt.customMessage();

                                    if (discoMsg.id().equals(msg.id())) {
                                        exchMgr.unregisterExchangeAwareComponent(this);

                                        partCntFut.onDone();
                                    }
                                }
                            }
                        }
                    }
                    );

                    partCntFut.listen(fut -> {
                        assert fut.error() == null : "Unexpected feature result [err=" + fut.error() + ']';

                        ignite.compute().executeAsync(taskCls, arg).listen(computeFut -> {
                            try {
                                resFut.onDone(computeFut.get());
                            }
                            catch (IgniteException e) {
                                resFut.onDone(e);
                            }
                        });
                    });

                    ignite.context().discovery().sendCustomEvent(msg);

                    if (ignite.log().isInfoEnabled())
                        ignite.log().info("Partition counter consistency reconciliation was started.");

                    fut = new IgniteFutureImpl<>(resFut);
                }
                else {
                    if (ignite.log().isInfoEnabled()) {
                        ignite.log().info("Partition counter consistency reconciliation was not started " +
                            "due to partition reconciliation task arguments.");

                        fut = ignite.compute().executeAsync(taskCls, arg);
                    }
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            if (!fut.isDone()) {
                jobCtx.holdcc();

                fut.listen((IgniteInClosure<IgniteFuture<ResultT>>)f -> jobCtx.callcc());

                return null;
            }
        }

        return fut.get();
    }
}

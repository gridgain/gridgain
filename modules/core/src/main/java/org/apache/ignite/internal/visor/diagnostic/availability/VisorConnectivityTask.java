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

package org.apache.ignite.internal.visor.diagnostic.availability;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteCallable;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@GridInternal
public class VisorConnectivityTask
    extends VisorMultiNodeTask<VisorConnectivityArgs, Map<ClusterNode, VisorConnectivityResult>, VisorConnectivityResult> {
    /** {@inheritDoc} */
    @Override protected VisorJob<VisorConnectivityArgs, VisorConnectivityResult> job(VisorConnectivityArgs arg) {
        return new VisorConnectivityJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<ClusterNode, VisorConnectivityResult> reduce0(
        List<ComputeJobResult> results) throws IgniteException {
        return results.stream().collect(Collectors.toMap(ComputeJobResult::getNode, ComputeJobResult::getData));
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<VisorConnectivityArgs> arg) {
        return arg.getArgument().getNodeIds();
    }

    /**
     * This job is sent to every node in cluster. It then use compute on every other node just to check
     * that there is a connection between nodes.
     */
    private static class VisorConnectivityJob extends VisorJob<VisorConnectivityArgs, VisorConnectivityResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg   Formal job argument.
         * @param debug Debug flag.
         */
        private VisorConnectivityJob(VisorConnectivityArgs arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorConnectivityResult run(VisorConnectivityArgs arg) {
            List<UUID> ids = arg.getNodeIds().stream()
                .filter(uuid -> !Objects.equals(ignite.configuration().getNodeId().toString(), uuid.toString()))
                .collect(Collectors.toList());

            Map<String, ConnectivityStatus> unavailableNodes = new ArrayList<>(ids).stream()
                .collect(Collectors.toMap(UUID::toString, uuid -> {
                    ClusterGroup node = ignite.cluster().forNodeId(uuid);

                    if (node.nodes().isEmpty())
                        return ConnectivityStatus.MISSING;

                    String res;

                    try {
                        res = ignite.compute(node).call(new CheckNodesCallable());
                    }
                    catch (IgniteException e) {
                        return ConnectivityStatus.UNAVAILABLE;
                    }

                    if (!"OK".equals(res))
                        return ConnectivityStatus.UNAVAILABLE;

                    return ConnectivityStatus.OK;
                }));

            return new VisorConnectivityResult(unavailableNodes);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorConnectivityJob.class, this);
        }
    }

    /**
     * Simple compute task that just returns string "OK"
     */
    private static class CheckNodesCallable implements IgniteCallable<String> {

        /** {@inheritDoc} */
        @Override public String call() throws Exception {
            return "OK";
        }

    }

}

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

package org.apache.ignite.internal.visor.dr;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.jetbrains.annotations.Nullable;

/**
 * Task to collect cache query metrics.
 */
@GridInternal
public class VisorDrRepairPartitionCountersTask extends VisorDrPartitionCountersTask<VisorDrRepairPartitionCountersTaskArg,
        VisorDrRepairPartitionCountersTaskResult, Collection<VisorDrRepairPartitionCountersJobResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected Set<String> getCaches(VisorDrRepairPartitionCountersTaskArg args) {
        return args.getCaches();
    }

    @Override
    protected VisorJob<VisorDrRepairPartitionCountersTaskArg, Collection<VisorDrRepairPartitionCountersJobResult>> createJob(
            VisorDrRepairPartitionCountersTaskArg args, Map<String, Set<Integer>> cachePartsMap,
            boolean debug) {
        return new DrRepairPartitionCountersJob(args, cachePartsMap, debug, true);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected VisorDrRepairPartitionCountersTaskResult reduce0(List<ComputeJobResult> results)
            throws IgniteException {
        Map<UUID, Collection<VisorDrCheckPartitionCountersJobResult>> nodeMetricsMap = new HashMap<>();
        Map<UUID, Exception> exceptions = new HashMap<>();

        for (ComputeJobResult res : results) {
            if (res.getException() != null) {
                exceptions.put(res.getNode().id(), res.getException());
            } else {
                Collection<VisorDrCheckPartitionCountersJobResult> metrics = res.getData();

                nodeMetricsMap.put(res.getNode().id(), metrics);
            }
        }

       return new VisorDrRepairPartitionCountersTaskResult(nodeMetricsMap, exceptions);
    }

}

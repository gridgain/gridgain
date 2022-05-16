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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;

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

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorDrRepairPartitionCountersTaskArg, Collection<VisorDrRepairPartitionCountersJobResult>> createJob(
            VisorDrRepairPartitionCountersTaskArg args, Map<String, Set<Integer>> cachePartsMap,
            boolean debug) {
        return new VisorDrRepairPartitionCountersJob(args, cachePartsMap, debug);
    }

    /** {@inheritDoc} */
    @Override protected VisorDrRepairPartitionCountersTaskResult createResult(Map<UUID, Exception> exceptions,
            Map<UUID, Collection<VisorDrRepairPartitionCountersJobResult>> results) {
        return new VisorDrRepairPartitionCountersTaskResult(results, exceptions);
    }
}

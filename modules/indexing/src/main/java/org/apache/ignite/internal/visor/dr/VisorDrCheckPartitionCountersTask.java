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
 * Task for check partition counter in DR entries.
 */
@GridInternal
public class VisorDrCheckPartitionCountersTask extends VisorDrPartitionCountersTask<VisorDrCheckPartitionCountersTaskArg,
        VisorDrCheckPartitionCountersTaskResult, Collection<VisorDrCheckPartitionCountersJobResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected Set<String> getCaches(VisorDrCheckPartitionCountersTaskArg args) {
        return args.getCaches();
    }

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorDrCheckPartitionCountersTaskArg, Collection<VisorDrCheckPartitionCountersJobResult>> createJob(
            VisorDrCheckPartitionCountersTaskArg args, Map<String, Set<Integer>> cachePartsMap,
            boolean debug) {
        return new VisorDrCheckPartitionCountersJob(args, debug, cachePartsMap);
    }

    /** {@inheritDoc} */
    @Override protected VisorDrCheckPartitionCountersTaskResult createResult(Map<UUID, Exception> exceptions,
            Map<UUID, Collection<VisorDrCheckPartitionCountersJobResult>> results) {
        return new VisorDrCheckPartitionCountersTaskResult(results, exceptions);
    }
}

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

package org.apache.ignite.internal.visor.compute;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.VISOR_TASK_EVTS;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.checkExplicitEvents;

/**
 * Task to run gc on nodes.
 */
@GridInternal
public class VisorComputeToggleMonitoringTask extends
    VisorMultiNodeTask<VisorComputeToggleMonitoringTaskArg, Boolean, Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Nullable @Override protected Boolean reduce0(List<ComputeJobResult> results) {
        Collection<Boolean> toggles = new HashSet<>();

        for (ComputeJobResult res : results)
            toggles.add(res.<Boolean>getData());

        // If all nodes return same result.
        return toggles.size() == 1;
    }

    /** {@inheritDoc} */
    @Override protected VisorComputeToggleMonitoringJob job(VisorComputeToggleMonitoringTaskArg arg) {
        return new VisorComputeToggleMonitoringJob(arg, debug);
    }

    /**
     * Job to toggle task monitoring on node.
     */
    private static class VisorComputeToggleMonitoringJob extends VisorJob<VisorComputeToggleMonitoringTaskArg, Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Visor ID key and monitoring state flag.
         * @param debug Debug flag.
         */
        private VisorComputeToggleMonitoringJob(VisorComputeToggleMonitoringTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Boolean run(VisorComputeToggleMonitoringTaskArg arg) {
            if (checkExplicitEvents(ignite, VISOR_TASK_EVTS))
                return Boolean.TRUE;

            VisorComputeMonitoringHolder holder = VisorComputeMonitoringHolder.getInstance(ignite);

            String visorKey = arg.getVisorKey();

            boolean state = arg.isEnabled();

            // Set task monitoring state.
            if (state)
                holder.startCollect(ignite, visorKey, VISOR_TASK_EVTS);
            else
                holder.stopCollect(ignite, visorKey);

            // Return actual state. It could stay the same if events explicitly enabled in configuration.
            return ignite.allEventsUserRecordable(VISOR_TASK_EVTS);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorComputeToggleMonitoringJob.class, this);
        }
    }
}

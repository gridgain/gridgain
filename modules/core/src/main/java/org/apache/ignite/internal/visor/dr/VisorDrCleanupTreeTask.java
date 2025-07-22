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

package org.apache.ignite.internal.visor.dr;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.tree.updatelog.PartitionLogTree.PART_LOG_TREE_CLEANUP_MNTC_TASK_NAME;
import static org.apache.ignite.internal.processors.cache.tree.updatelog.PartitionLogTree.TASK_DESCRIPTION;
import static org.apache.ignite.internal.processors.cache.tree.updatelog.PartitionLogTree.mergeCleanupTasks;
import static org.apache.ignite.internal.processors.cache.tree.updatelog.PartitionLogTree.toCleanupMaintenanceTask;

/**  */
@GridInternal
@GridVisorManagementTask
public class VisorDrCleanupTreeTask extends VisorMultiNodeTask<VisorDrCleanupTreeTaskArgs, VisorDrCleanupTreeTaskResult, VisorDrCleanupTreeTaskResult> {
    /**  */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorDrCleanupTreeTaskArgs, VisorDrCleanupTreeTaskResult> job(
        VisorDrCleanupTreeTaskArgs arg) {
        return new CleanupJob(arg, debug);
    }

    @Override protected @Nullable VisorDrCleanupTreeTaskResult reduce0(List<ComputeJobResult> results) throws IgniteException {
        if (taskArg.op() == VisorDrRebuildTreeOperation.DEFAULT) {
            StringBuilder msg = new StringBuilder();

            for (ComputeJobResult res : results) {
                msg.append(res.getNode().consistentId()).append(":\n");

                if (res.getData() == null)
                    msg.append("    err=").append(res.getException()).append('\n');
                else {
                    VisorDrCleanupTreeTaskResult data = res.getData();

                    msg.append("    success=").append(data.isSuccess()).append('\n');
                    msg.append("    msg=").append(String.join(";", data.messages())).append('\n');
                }
            }

            return new VisorDrCleanupTreeTaskResult(true, msg.toString());
        }

        assert results.size() == 1;

        ComputeJobResult res = results.get(0);

        if (res.getException() == null)
            return res.getData();

        throw res.getException();
    }

    /**  */
    private static class CleanupJob extends VisorJob<VisorDrCleanupTreeTaskArgs, VisorDrCleanupTreeTaskResult> {
        /**  */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg   Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        CleanupJob(@Nullable VisorDrCleanupTreeTaskArgs arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorDrCleanupTreeTaskResult run(
            @Nullable VisorDrCleanupTreeTaskArgs arg) throws IgniteException {
            if (arg != null) {
                switch (arg.op()) {
                    case STATUS:
                        return runStatus();
                    case CANCEL:
                        return runCancel();
                    case DEFAULT:
                        break;
                }
            }

            Set<Integer> groupIds = new HashSet<>();
            List<String> failures = new ArrayList<>();

            if (arg != null)
                extractGroupIds(arg, groupIds::add, failures::add);

            if (groupIds.isEmpty() && failures.isEmpty()) {
                groupIds = ignite.context().cache().cacheGroups().stream()
                    .filter(CacheGroupContext::isDrEnabled)
                    .map(CacheGroupContext::groupId)
                    .collect(Collectors.toSet());

                if (groupIds.isEmpty())
                    failures.add("No cache groups with DR caches found.");
            }

            if (groupIds.isEmpty())
                return new VisorDrCleanupTreeTaskResult(failures);

            MaintenanceRegistry mntcReg = ignite.context().maintenanceRegistry();

            if (mntcReg.isMaintenanceMode())
                return runMaintenanceTask(groupIds, mntcReg);
            else
                return scheduleMaintenanceTask(groupIds, mntcReg);
        }

        /**
         * Schedules maintenance task for cleaning partition tries.
         *
         * @param groupIds Group id, which partitions has to be clean.
         * @param mntcReg  Maintenance registry.
         * @return Visor task result.
         */
        private VisorDrCleanupTreeTaskResult scheduleMaintenanceTask(
            Set<Integer> groupIds,
            MaintenanceRegistry mntcReg
        ) {
            try {
                MaintenanceTask task = toCleanupMaintenanceTask(groupIds);

                mntcReg.registerMaintenanceTask(task, oldTask -> mergeCleanupTasks(oldTask, task));

                return new VisorDrCleanupTreeTaskResult(true, "Maintenance task was (re)scheduled: " + taskName());
            }
            catch (IgniteCheckedException ex) {
                ignite.log().warning("Failed to register maintenance record for corrupted partition files.", ex);

                return new VisorDrCleanupTreeTaskResult(false, "Failed schedule maintenance task: " + taskName());
            }
        }

        /**
         * Runs maintenance task for cleaning partition tries.
         *
         * @param groupIds Group id, which partitions has to be cleaned.
         * @param mntcReg  Maintenance registry.
         * @return Visor task result.
         */
        private VisorDrCleanupTreeTaskResult runMaintenanceTask(Set<Integer> groupIds, MaintenanceRegistry mntcReg) {
            if (mntcReg.requestedTask(PART_LOG_TREE_CLEANUP_MNTC_TASK_NAME) != null)
                return new VisorDrCleanupTreeTaskResult(false, "Maintenance task is already in progress: " + taskName());

            try {
                MaintenanceTask task = toCleanupMaintenanceTask(groupIds);

                mntcReg.registerMaintenanceTask(task, oldTask -> mergeCleanupTasks(oldTask, task));

                mntcReg.actionsForMaintenanceTask(PART_LOG_TREE_CLEANUP_MNTC_TASK_NAME)
                    .forEach(MaintenanceAction::execute);

                mntcReg.unregisterMaintenanceTask(PART_LOG_TREE_CLEANUP_MNTC_TASK_NAME);

                return new VisorDrCleanupTreeTaskResult(true, "Maintenance task finished: " + taskName());
            }
            catch (IgniteCheckedException ex) {
                ignite.log().warning("Failed to register maintenance record for corrupted partition files.", ex);

                return new VisorDrCleanupTreeTaskResult(false, "Failed run maintenance task: " + taskName());
            }
        }

        /**
         * Cancels maintenance task that was previously scheduled.
         *
         * @return Visor task result.
         */
        private VisorDrCleanupTreeTaskResult runCancel() {
            MaintenanceRegistry mntcReg = ignite.context().maintenanceRegistry();

            if (mntcReg.isMaintenanceMode()) {
                return new VisorDrCleanupTreeTaskResult(false, "Maintenance task can't be cancelled while " +
                    "in progress: " + taskName());
            }
            else if (mntcReg.unregisterMaintenanceTask(PART_LOG_TREE_CLEANUP_MNTC_TASK_NAME))
                return new VisorDrCleanupTreeTaskResult(true, "Maintenance task is cancelled: " + taskName());
            else {
                return new VisorDrCleanupTreeTaskResult(false, "Nothing to do. Maintenance task wasn't " +
                    "scheduled: " + taskName());
            }
        }

        /**
         * Return maintenance task status.
         *
         * @return Visor task result.
         */
        private VisorDrCleanupTreeTaskResult runStatus() {
            MaintenanceRegistry mntcReg = ignite.context().maintenanceRegistry();

            MaintenanceTask task = mntcReg.requestedTask(PART_LOG_TREE_CLEANUP_MNTC_TASK_NAME);
            boolean inMaintenanceMode = mntcReg.isMaintenanceMode();

            String status;

            if (inMaintenanceMode) {
                status = task != null ?
                    taskName() + " task is in progress: [params=" + task.parameters() + ']' :
                    taskName() + " was finished or never scheduled.";
            }
            else {
                status = task != null ?
                    taskName() + " task is scheduled: [params=" + task.parameters() + ']' :
                    taskName() + " is not scheduled.";
            }

            return new VisorDrCleanupTreeTaskResult(false, status);
        }

        /**
         * Process task arguments and extract cache group id.
         *
         * @param arg      Task arguments.
         * @param groupIds Group id collector.
         * @param failures Failures collector.
         */
        private void extractGroupIds(VisorDrCleanupTreeTaskArgs arg, Consumer<Integer> groupIds,
            Consumer<String> failures) {
            GridCacheProcessor cacheProc = ignite.context().cache();

            if (!F.isEmpty(arg.caches())) {
                arg.caches().forEach(cacheName -> {
                        DynamicCacheDescriptor desc = cacheProc.cacheDescriptor(cacheName);

                        if (desc != null)
                            groupIds.accept(desc.groupId());
                        else
                            failures.accept("Failed to find cache: " + cacheName);
                    }
                );
            }

            if (!F.isEmpty(arg.groups())) {
                arg.groups().forEach(grpName -> {
                        CacheGroupDescriptor desc = cacheProc.cacheGroupDescriptor(CU.cacheId(grpName));

                        if (desc != null)
                            groupIds.accept(desc.groupId());
                        else
                            failures.accept("Failed to find cache group: " + grpName);
                    }
                );
            }
        }

        /**
         * @return Maintenance task name.
         */
        private String taskName() {
            return TASK_DESCRIPTION;
        }
    }
}

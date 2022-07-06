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

package org.apache.ignite.internal.processors.cache.tree.updatelog;

import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.maintenance.MaintenanceTask;

/**
 *
 */
public class PartitionLogTree extends BPlusTree<UpdateLogRow, UpdateLogRow> {
    /** Index rebuild maintenance task name. */
    public static final String PART_LOG_TREE_REBUILD_MNTC_TASK_NAME = "PartitionLogTreeRebuildMaintenanceTask";

    /** Maintenance task parameters separator. */
    public static final String PARAMETER_SEPARATOR = "|";

    /** Maintenance task parameters separator regexp. */
    public static final String PARAMETER_SEPARATOR_REGEX = "\\|";

    /** Maintenance task description. */
    private static final String TASK_DESCRIPTION = "Partition log tree rebuild";

    /** */
    public static final Object FULL_ROW = new Object();

    /** */
    private final CacheGroupContext grp;

    /** */
    private final int part;

    /** */
    private final boolean strictConsistencyCheck = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_STRICT_CONSISTENCY_CHECK);

    /** */
    private final IgniteLogger log;

    /**
     * @param grp Cache group.
     * @param part Partition.
     * @param name Tree name.
     * @param pageMem Page memory.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param initNew Initialize new index.
     * @param pageLockTrackerManager Page lock tracker manager.
     * @param pageFlag Default flag value for allocated pages.
     * @param log Logger.
     * @throws IgniteCheckedException If failed.
     */
    public PartitionLogTree(
        CacheGroupContext grp,
        int part,
        String name,
        PageMemory pageMem,
        long metaPageId,
        ReuseList reuseList,
        boolean initNew,
        PageLockTrackerManager pageLockTrackerManager,
        byte pageFlag,
        IgniteLogger log
    ) throws IgniteCheckedException {
        super(
            name,
            grp.groupId(),
            grp.name(),
            pageMem,
            grp.dataRegion().config().isPersistenceEnabled() ? grp.shared().wal() : null,
            grp.offheap().globalRemoveId(),
            metaPageId,
            reuseList,
            grp.sharedGroup() ? CacheIdAwareUpdateLogInnerIO.VERSIONS : UpdateLogInnerIO.VERSIONS,
            grp.sharedGroup() ? CacheIdAwareUpdateLogLeafIO.VERSIONS : UpdateLogLeafIO.VERSIONS,
            pageFlag,
            grp.shared().kernalContext().failure(),
            pageLockTrackerManager
        );

        this.part = part;
        this.grp = grp;
        this.log = log;

        assert !grp.dataRegion().config().isPersistenceEnabled() || grp.shared().database().checkpointLockIsHeldByThread();

        initTree(initNew);
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<UpdateLogRow> iox, long pageAddr, int idx, UpdateLogRow row) {
        UpdateLogRowIO io = (UpdateLogRowIO)iox;

        int cmp;

        if (grp.sharedGroup()) {
            assert row.cacheId != CU.UNDEFINED_CACHE_ID : "Cache ID is not provided!";
            assert io.getCacheId(pageAddr, idx) != CU.UNDEFINED_CACHE_ID : "Cache ID is not stored!";

            cmp = Integer.compare(io.getCacheId(pageAddr, idx), row.cacheId);

            if (cmp != 0)
                return cmp;

            if (row.updCntr == 0 && row.link == 0) {
                // A search row with a cache ID only is used as a cache bound.
                // The found position will be shifted until the exact cache bound is found;
                // See for details:
                // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findLowerBound()
                // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findUpperBound()
                return cmp;
            }
        }

        long updCntr = io.getUpdateCounter(pageAddr, idx);

        cmp = Long.compare(updCntr, row.updCntr);

        /* remove row */
        if (cmp == 0 && row.link != 0 /* search insertion point */ && io.getLink(pageAddr, idx) != row.link) {
            String msg = "Duplicate update counter at update log tree [" + "grp=" + grp.cacheOrGroupName() + ", part="
                + part + ", updCounter=" + updCntr + +']';

            // Update counter is unique number assigned to a row on per-partition basis.
            // Duplicate value (caused by bugs or whatever) may affect other components, which relies on that invariant.
            // However, a user controls (via system property) whether the violence is critical or not.
            if (strictConsistencyCheck)
                throw new DuplicateUpdateCounterException(msg);
            else
                log.warning(msg);
        }

        return cmp;
    }

    /** {@inheritDoc} */
    @Override public UpdateLogRow getRow(BPlusIO<UpdateLogRow> io, long pageAddr, int idx, Object flag)
        throws IgniteCheckedException {
        UpdateLogRow row = io.getLookupRow(this, pageAddr, idx);

        return flag == FULL_ROW ? row.initRow(grp) : row;
    }

    /**
     * Construct the exception and invoke failure processor.
     *
     * @param msg Message.
     * @param cause Cause.
     * @param grpId Group id.
     * @param pageIds Pages ids.
     * @return New CorruptedTreeException instance.
     */
    @Override protected CorruptedTreeException corruptedTreeException(String msg, Throwable cause, int grpId,
        long... pageIds) {
        if (cause instanceof DuplicateUpdateCounterException) {
            // Duplicate update counter doesn't mean the tree is corrupted.
            return super.corruptedTreeException(msg, cause, grpId, pageIds);
        }

        CorruptedTreeException e = new CorruptedTreeException(msg, cause, grpName, null, name(), grpId, pageIds);

        String errorMsg = "Partition log tree of partition `" + part + "` of cache group `" + grpName + "` is corrupted, " +
            "to fix this issue a rebuild is required. On the next restart, node will enter " +
            "the maintenance mode and rebuild corrupted tree.";

        log.warning(errorMsg);

        try {
            MaintenanceTask task = toMaintenanceTask(grpId);

            grp.shared().kernalContext().maintenanceRegistry().registerMaintenanceTask(task,
                oldTask -> mergeTasks(oldTask, task));
        }
        catch (IgniteCheckedException ex) {
            log.warning("Failed to register maintenance record for corrupted partition files.", ex);
        }

        processFailure(FailureType.CRITICAL_ERROR, e);

        return e;
    }

    /**
     * Constructs a partition log tree rebuild maintenance task.
     *
     * @param groupId Group id.
     * @return Maintenance task.
     */
    public static MaintenanceTask toMaintenanceTask(int groupId) {
        return new MaintenanceTask(PART_LOG_TREE_REBUILD_MNTC_TASK_NAME, TASK_DESCRIPTION,
            U.hexInt(groupId));
    }

    /**
     * Merges two index rebuild maintenance tasks concatenating their parameters.
     *
     * @param oldTask Old task
     * @param newTask New task.
     * @return Merged task.
     */
    public static MaintenanceTask mergeTasks(MaintenanceTask oldTask, MaintenanceTask newTask) {
        assert Objects.equals(PART_LOG_TREE_REBUILD_MNTC_TASK_NAME, oldTask.name());
        assert Objects.equals(oldTask.name(), newTask.name());

        String oldTaskParams = oldTask.parameters();
        String newTaskParams = newTask.parameters();

        if (oldTaskParams.contains(newTaskParams))
            return oldTask;

        String mergedParams = oldTaskParams + PARAMETER_SEPARATOR + newTaskParams;

        return new MaintenanceTask(oldTask.name(), oldTask.description(), mergedParams);
    }
}

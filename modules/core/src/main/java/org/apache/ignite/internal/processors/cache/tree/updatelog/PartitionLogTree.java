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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.SystemProperty;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.maintenance.MaintenanceTask;

import static java.lang.Long.toHexString;

/**
 *
 */
public class PartitionLogTree extends BPlusTree<UpdateLogRow, UpdateLogRow> {
    /** Partition log tree rebuild maintenance task name. */
    public static final String PART_LOG_TREE_REBUILD_MNTC_TASK_NAME = "PartitionLogTreeRebuildMaintenanceTask";

    /** Partition log tree cleanup maintenance task name. */
    public static final String PART_LOG_TREE_CLEANUP_MNTC_TASK_NAME = "PartitionLogTreeCleanupMaintenanceTask";

    /** Maintenance task parameters separator. */
    public static final String PARAMETER_SEPARATOR = "|";

    /** Maintenance task parameters separator regexp. */
    public static final String PARAMETER_SEPARATOR_REGEX = "\\|";

    /** Maintenance task description. */
    public static final String TASK_DESCRIPTION = "Partition log tree rebuild";

    /** Maintenance task description. */
    public static final String CLEANUP_TASK_DESCRIPTION = "Partition log tree cleanup";

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

    /** */
    public static final int DFLT_PART_NUMBER_TO_DEBUG = 467;

    /** */
    @SystemProperty(value = "Partition number to print reuse lists details", type = Long.class,
        defaults = "" + DFLT_PART_NUMBER_TO_DEBUG)
    public static final String PART_NUMBER_TO_DEBUG = "PART_NUMBER_TO_DEBUG";

    /** */
    private static final int PART_NUMBER = IgniteSystemProperties.getInteger(PART_NUMBER_TO_DEBUG, DFLT_PART_NUMBER_TO_DEBUG);

    /** */
    public static final String DFLT_CACHE_OR_GROUP_NAME_TO_DEBUG = "com.aexp.rc.authorization.Authorization";

    /** */
    @SystemProperty(value = "Cache or group name to print reuse lists details", type = Long.class,
        defaults = "" + DFLT_CACHE_OR_GROUP_NAME_TO_DEBUG)
    public static final String CACHE_OR_GROUP_NAME_TO_DEBUG = "CACHE_OR_GROUP_NAME_TO_DEBUG";

    /** */
    private static final String CACHE_OR_GROUP_NAME = IgniteSystemProperties.getString(CACHE_OR_GROUP_NAME_TO_DEBUG, DFLT_CACHE_OR_GROUP_NAME_TO_DEBUG);

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

        if (PART_NUMBER == -1 || (part == PART_NUMBER && Objects.equals(grp.cacheOrGroupName(), CACHE_OR_GROUP_NAME))) {
            printReuseList();
        }

        initTree(initNew);
    }

    private void printReuseList() throws IgniteCheckedException {
        List<PagesList.Stripe> stripes = ((AbstractFreeList)reuseList).getStripes();

        log.info(String.format("cacheOrGroupName=%s, groupId=%s, partId=%s", grp.cacheOrGroupName(), grpId, part));

        if (reuseList instanceof PagesList)
            printMeta(((PagesList)reuseList).metaPageId());

        for (PagesList.Stripe stripe : stripes) {
            long tailId = stripe.tailId;

            long nextId;
            long previousId;

            long tailPage = acquirePage(tailId, IoStatisticsHolderNoOp.INSTANCE);

            try {
                long tailAddr = readLock(tailId, tailPage);

                try {
                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(tailAddr);
                    nextId = io.getNextId(tailAddr);
                    previousId = io.getPreviousId(tailAddr);

                    log.info(String.format(
                        "reuseListStripe=[tailId=%s, empty=%s, nextId=%s, previousId=%s]",
                        toHexString(tailId),
                        stripe.empty,
                        toHexString(nextId),
                        toHexString(previousId)
                    ));
                } finally {
                    readUnlock(tailId, tailPage, tailAddr);
                }
            } finally {
                releasePage(tailId, tailPage);
            }

            printStripeList("prev", previousId, PagesListNodeIO::getPreviousId);
            printStripeList("next", nextId, PagesListNodeIO::getNextId);
        }
    }

    private void printMeta(long metaPageId) throws IgniteCheckedException {
        while (metaPageId != 0) {
            long nextPageId;
            long metaPage = acquirePage(metaPageId, IoStatisticsHolderNoOp.INSTANCE);

            try {
                long metaAddr = readLock(metaPageId, metaPage);

                if (metaAddr == 0) {
                    log.warning(String.format("meta-lock-failed[pageId=%s]", toHexString(metaPageId)));

                    return;
                }

                try {
                    PagesListMetaIO io = PagesListMetaIO.VERSIONS.forPage(metaAddr);

                    Map<Integer, GridLongList> map = new TreeMap<>();
                    Map<Integer, String[]> strMap = new TreeMap<>();
                    io.getBucketsData(metaAddr, map);
                    for (Map.Entry<Integer, GridLongList> entry : map.entrySet()) {
                        GridLongList longLost = entry.getValue();
                        String[] strList = new String[longLost.size()];
                        for (int i = 0; i < longLost.size(); i++) {
                            strList[i] = toHexString(longLost.get(i));
                        }
                        strMap.put(entry.getKey(), strList);
                    }

                    long nextMetaPageId = io.getNextMetaPageId(metaAddr);

                    log.info(String.format(
                        "meta[pageId=%s, nextMetaPageId=%s, pages=%s]",
                        toHexString(metaPageId),
                        toHexString(nextMetaPageId),
                        strMap
                    ));

                    nextPageId = nextMetaPageId;
                } finally {
                    readUnlock(metaPageId, metaPage, metaAddr);
                }
            } finally {
                releasePage(metaPageId, metaPage);
            }

            metaPageId = nextPageId;
        }
    }

    private void printStripeList(String prefix, long curPageId, IgniteBiClosure<PagesListNodeIO, Long, Long> next) throws IgniteCheckedException {
        Set<Long> dejaVu = new HashSet<>();

        while (curPageId != 0) {
            if (!dejaVu.add(curPageId)) {
                log.warning(String.format(prefix + "-cycle-detected[pageId=%s]", toHexString(curPageId)));

                return;
            }

            long page = acquirePage(curPageId, IoStatisticsHolderNoOp.INSTANCE);

            long nextPageId;
            try {
                long addr = readLock(curPageId, page);

                if (addr == 0) {
                    log.warning(String.format(prefix + "-lock-failed[pageId=%s]", toHexString(curPageId)));

                    return;
                }

                try {
                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(addr);

                    long nextId = io.getNextId(addr);
                    long previousId = io.getPreviousId(addr);

                    nextPageId = next.apply(io, addr);

                    log.info(String.format(
                        prefix + "[pageId=%s, nextId=%s, previousId=%s]",
                        toHexString(curPageId),
                        toHexString(nextId),
                        toHexString(previousId)
                    ));
                } finally {
                    readUnlock(curPageId, page, addr);
                }
            } finally {
                releasePage(curPageId, page);
            }

            curPageId = nextPageId;
        }
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
     * Constructs a partition log tree rebuild maintenance task.
     *
     * @param groupIds Group id set.
     * @return Maintenance task.
     */
    public static MaintenanceTask toMaintenanceTask(Set<Integer> groupIds) {
        assert !groupIds.isEmpty();

        return new MaintenanceTask(PART_LOG_TREE_REBUILD_MNTC_TASK_NAME, TASK_DESCRIPTION,
            groupIds.stream().map(U::hexInt).collect(Collectors.joining(PARAMETER_SEPARATOR)));
    }

    /**
     * Constructs a partition log tree cleanup maintenance task.
     *
     * @param groupIds Group id set.
     * @return Maintenance task.
     */
    public static MaintenanceTask toCleanupMaintenanceTask(Set<Integer> groupIds) {
        assert !groupIds.isEmpty();

        return new MaintenanceTask(PART_LOG_TREE_CLEANUP_MNTC_TASK_NAME, CLEANUP_TASK_DESCRIPTION,
            groupIds.stream().map(U::hexInt).collect(Collectors.joining(PARAMETER_SEPARATOR)));
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

        return mergeTaskParameters(oldTask, newTask);
    }

    /**
     * Merges two tree cleanup maintenance tasks concatenating their parameters.
     *
     * @param oldTask Old task
     * @param newTask New task.
     * @return Merged task.
     */
    public static MaintenanceTask mergeCleanupTasks(MaintenanceTask oldTask, MaintenanceTask newTask) {
        assert Objects.equals(PART_LOG_TREE_CLEANUP_MNTC_TASK_NAME, oldTask.name());
        assert Objects.equals(oldTask.name(), newTask.name());

        return mergeTaskParameters(oldTask, newTask);
    }

    private static MaintenanceTask mergeTaskParameters(MaintenanceTask oldTask, MaintenanceTask newTask) {
        String oldTaskParams = oldTask.parameters();
        String newTaskParams = newTask.parameters();

        assert oldTaskParams != null;
        assert newTaskParams != null;

        if (oldTaskParams.contains(newTaskParams))
            return oldTask;

        String mergedParams = Stream.concat(
                Arrays.stream(oldTaskParams.split(PARAMETER_SEPARATOR_REGEX)),
                Arrays.stream(newTaskParams.split(PARAMETER_SEPARATOR_REGEX)))
            .distinct()
            .collect(Collectors.joining(PARAMETER_SEPARATOR));

        return new MaintenanceTask(oldTask.name(), oldTask.description(), mergedParams);
    }
}

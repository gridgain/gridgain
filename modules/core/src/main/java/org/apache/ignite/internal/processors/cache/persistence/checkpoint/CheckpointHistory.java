/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupContextSupplier;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntry.GroupState;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.EarliestCheckpointMapSnapshot.GroupStateSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.util.lang.IgniteThrowableBiPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;

/**
 * Checkpoint history. Holds chronological ordered map with {@link CheckpointEntry CheckpointEntries}. Data is loaded
 * from corresponding checkpoint directory. This directory holds files for checkpoint start and end.
 */
public class CheckpointHistory {
    /** @see IgniteSystemProperties#IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE */
    public static final int DFLT_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE = 100;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Maps checkpoint's timestamp (from CP file name) to CP entry. Using TS provides historical order of CP entries in
     * map ( first is oldest )
     */
    private final NavigableMap<Long, CheckpointEntry> histMap = new ConcurrentSkipListMap<>();

    /** The maximal number of checkpoints hold in memory. */
    private final int maxCpHistMemSize;

    /** Should WAL be truncated */
    private final boolean isWalTruncationEnabled;

    /** Write ahead log manager. */
    private final IgniteWriteAheadLogManager wal;

    /** Checking that checkpoint is applicable or not for given cache group. */
    private final IgniteThrowableBiPredicate</*Checkpoint timestamp*/Long, /*Group id*/Integer> checkpointInapplicable;

    /** It is available or not to reserve checkpoint(deletion protection). */
    private final boolean reservationDisabled;

    /** Cache group IDs for which the earliest checkpoint timestamp for partitions is known. */
    private final Set<Integer> earliestCpGrps = ConcurrentHashMap.newKeySet();

    /** Cache group context supplier. */
    private final CacheGroupContextSupplier cacheGrpCtxSupplier;

    /** Internal snapshot of the earliest checkpoint to restore from. */
    private final AtomicReference<EarliestCheckpointMapSnapshot> earliestCpSnapshot = new AtomicReference<>();

    /**
     * Constructor.
     *
     * @param dsCfg Data storage configuration.
     * @param logFun Function for getting a logger.
     * @param wal WAL manager.
     * @param inapplicable Checkpoint inapplicable filter.
     * @param cacheGrpCtxSupplier Cache group context supplier.
     */
    CheckpointHistory(
        DataStorageConfiguration dsCfg,
        Function<Class<?>, IgniteLogger> logFun,
        IgniteWriteAheadLogManager wal,
        IgniteThrowableBiPredicate<Long, Integer> inapplicable,
        CacheGroupContextSupplier cacheGrpCtxSupplier
    ) {
        this.log = logFun.apply(getClass());
        this.wal = wal;
        this.checkpointInapplicable = inapplicable;
        this.cacheGrpCtxSupplier = cacheGrpCtxSupplier;

        isWalTruncationEnabled = dsCfg.getMaxWalArchiveSize() != DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;

        maxCpHistMemSize = IgniteSystemProperties.getInteger(IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE,
            DFLT_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE);

        reservationDisabled = dsCfg.getWalMode() == WALMode.NONE;
    }

    /**
     * Initializes the checkpoint history.
     *
     * <p>Updates internal structures.
     *
     * @param checkpoints Checkpoints.
     * @param snapshot Earliest checkpoint map snapshot.
     */
    void initialize(
        List<CheckpointEntry> checkpoints,
        EarliestCheckpointMapSnapshot snapshot
    ) {
        for (CheckpointEntry e : checkpoints)
            histMap.put(e.timestamp(), e);

        boolean casRes = earliestCpSnapshot.compareAndSet(null, snapshot);

        assert casRes;
    }

    /**
     * Starts checkpoint history.
     *
     * <p>Applies a previously taken (on initialization or deactivation) snapshot of the earliest checkpoint.
     */
    void start() {
        applyEarliestCpSnapshot();
    }

    /**
     * @param cpTs Checkpoint timestamp.
     * @return Initialized entry.
     * @throws IgniteCheckedException If failed to initialize entry.
     */
    private CheckpointEntry entry(Long cpTs) throws IgniteCheckedException {
        CheckpointEntry entry = histMap.get(cpTs);

        if (entry == null)
            throw new IgniteCheckedException("Checkpoint entry was removed: " + cpTs);

        return entry;
    }

    /**
     * @return First checkpoint entry if exists. Otherwise {@code null}.
     */
    public CheckpointEntry firstCheckpoint() {
        Map.Entry<Long, CheckpointEntry> entry = histMap.firstEntry();

        return entry != null ? entry.getValue() : null;
    }

    /**
     * @return Last checkpoint entry if exists.
     */
    @Nullable public CheckpointEntry lastCheckpoint() {
        Map.Entry<Long, CheckpointEntry> entry = histMap.lastEntry();

        return entry != null ? entry.getValue() : null;
    }

    /**
     * @return First checkpoint WAL pointer if exists. Otherwise {@code null}.
     */
    public WALPointer firstCheckpointPointer() {
        CheckpointEntry entry = firstCheckpoint();

        return entry != null ? entry.checkpointMark() : null;
    }

    /**
     * @return Collection of checkpoint timestamps.
     */
    public Collection<Long> checkpoints(boolean descending) {
        if (descending)
            return histMap.descendingKeySet();

        return histMap.keySet();
    }

    /**
     *
     */
    public Collection<Long> checkpoints() {
        return checkpoints(false);
    }

    /**
     * Adds checkpoint entry after the corresponding WAL record has been written to WAL. The checkpoint itself is not
     * finished yet.
     *
     * @param entry Entry to add.
     * @param cacheStates Cache states map.
     */
    public void addCheckpoint(CheckpointEntry entry, Map<Integer, CacheState> cacheStates) {
        addCpCacheStatesToEarliestCpMap(entry, cacheStates);

        histMap.put(entry.timestamp(), entry);
    }

    /**
     * Update map which stored the earliest checkpoint for groups.
     *
     * @param cp Checkpoint entry.
     * @param statesFromSnapshot Group state map from the snapshot.
     */
    private void updateEarliestCpMap(
        CheckpointEntry cp,
        @Nullable Map<Integer, GroupState> statesFromSnapshot
    ) {
        try {
            Map<Integer, GroupState> states = statesFromSnapshot != null ? statesFromSnapshot : cp.groupState(wal);

            Iterator<Integer> it = earliestCpGrps.iterator();

            while (it.hasNext()) {
                int grpId = it.next();

                if (!isCheckpointApplicableForGroup(grpId, cp)) {
                    it.remove();

                    clearEarliestCpTsOfGrp(grpId);

                    continue;
                }

                CacheGroupContext grpCtx = cacheGrpCtxSupplier.get(grpId);

                if (grpCtx == null)
                    continue;

                for (GridDhtLocalPartition localPart : grpCtx.topology().localPartitions()) {
                    if (states.get(grpId).indexByPartition(localPart.id()) < 0)
                        localPart.earliestCpTs(0);
                }
            }

            addCpGroupStatesToEarliestCpMap(cp, states);
        }
        catch (IgniteCheckedException ex) {
            U.warn(log, "Failed to process checkpoint: " + cp, ex);

            earliestCpGrps.clear();
        }
    }

    /**
     * Removes last checkpoint in history from the earliest checkpoints map by group id and returns the latest
     * checkpoint in the history.
     *
     * @param grpId Group id.
     * @return Latest checkpoint in the history.
     */
    public CheckpointEntry removeFromEarliestCheckpoints(Integer grpId) {
        synchronized (earliestCpGrps) {
            CheckpointEntry lastCp = lastCheckpoint();

            earliestCpGrps.remove(grpId);

            clearEarliestCpTsOfGrp(grpId);

            return lastCp;
        }
    }

    /**
     * Add last checkpoint to map of the earliest checkpoints.
     *
     * @param entry Checkpoint entry.
     * @param cacheStates Cache states map.
     */
    private void addCpCacheStatesToEarliestCpMap(CheckpointEntry entry, Map<Integer, CacheState> cacheStates) {
        for (Integer grpId : cacheStates.keySet()) {
            CacheState cacheState = cacheStates.get(grpId);

            for (int pIdx = 0; pIdx < cacheState.size(); pIdx++) {
                int partId = cacheState.partitionByIndex(pIdx);

                setLocalPartitionEarliestCpTs(grpId, partId, entry.timestamp());
            }
        }
    }

    /**
     * Add last checkpoint to map of the earliest checkpoints.
     *
     * @param entry Checkpoint entry.
     * @param cacheGrpStates Group states map.
     */
    private void addCpGroupStatesToEarliestCpMap(
        CheckpointEntry entry,
        Map<Integer, GroupState> cacheGrpStates
    ) {
        for (Integer grpId : cacheGrpStates.keySet()) {
            GroupState grpState = cacheGrpStates.get(grpId);

            for (int pIdx = 0; pIdx < grpState.size(); pIdx++) {
                int partId = grpState.getPartitionByIndex(pIdx);

                setLocalPartitionEarliestCpTs(grpId, partId, entry.timestamp());
            }
        }
    }

    /**
     * Clears checkpoint history after WAL truncation.
     *
     * @param ptr Upper bound.
     * @return List of checkpoint entries removed from history.
     */
    public List<CheckpointEntry> onWalTruncated(WALPointer ptr) {
        List<CheckpointEntry> removed = new ArrayList<>();

        FileWALPointer highBound = (FileWALPointer)ptr;

        for (CheckpointEntry cpEntry : histMap.values()) {
            FileWALPointer cpPnt = (FileWALPointer)cpEntry.checkpointMark();

            if (highBound.compareTo(cpPnt) <= 0)
                break;

            if (!removeCheckpoint(cpEntry))
                break;

            removed.add(cpEntry);
        }

        return removed;
    }

    /**
     * Removes checkpoints from history.
     *
     * @return List of checkpoint entries removed from history.
     */
    public List<CheckpointEntry> removeCheckpoints(int countToRemove) {
        if (countToRemove == 0)
            return Collections.emptyList();

        List<CheckpointEntry> removed = new ArrayList<>();

        for (Iterator<Map.Entry<Long, CheckpointEntry>> iterator = histMap.entrySet().iterator();
            iterator.hasNext() && removed.size() < countToRemove; ) {
            Map.Entry<Long, CheckpointEntry> entry = iterator.next();

            CheckpointEntry checkpoint = entry.getValue();

            if (!removeCheckpoint(checkpoint))
                break;

            removed.add(checkpoint);
        }

        return removed;
    }

    /**
     * Remove checkpoint from history.
     *
     * @param checkpoint Checkpoint to be removed.
     * @return Whether checkpoint was removed from history.
     */
    private boolean removeCheckpoint(CheckpointEntry checkpoint) {
        if (wal.reserved(checkpoint.checkpointMark())) {
            U.warn(log, "Could not clear historyMap due to WAL reservation on cp: " + checkpoint +
                ", history map size is " + histMap.size());

            return false;
        }

        synchronized (earliestCpGrps) {
            CheckpointEntry deletedCpEntry = histMap.remove(checkpoint.timestamp());

            CheckpointEntry oldestCpInHist = firstCheckpoint();

            earliestCpGrps.stream()
                .map(cacheGrpCtxSupplier::get)
                .filter(Objects::nonNull)
                .flatMap(grpCtx -> grpCtx.topology().localPartitions().stream())
                .filter(localPart -> localPart.earliestCpTs() == deletedCpEntry.timestamp())
                .forEach(localPart -> localPart.earliestCpTs(oldestCpInHist.timestamp()));
        }

        return true;
    }

    /**
     * Logs and clears checkpoint history after checkpoint finish.
     *
     * @param chp Finished checkpoint.
     * @return List of checkpoints removed from history.
     */
    public List<CheckpointEntry> onCheckpointFinished(Checkpoint chp) {
        chp.walSegsCoveredRange(calculateWalSegmentsCovered());

        return removeCheckpoints(isWalTruncationEnabled ? 0 : histMap.size() - maxCpHistMemSize);
    }

    /**
     * Calculates indexes of WAL segments covered by last checkpoint.
     *
     * @return list of indexes or empty list if there are no checkpoints.
     */
    private IgniteBiTuple<Long, Long> calculateWalSegmentsCovered() {
        IgniteBiTuple<Long, Long> tup = new IgniteBiTuple<>(-1L, -1L);

        Map.Entry<Long, CheckpointEntry> lastEntry = histMap.lastEntry();

        if (lastEntry == null)
            return tup;

        Map.Entry<Long, CheckpointEntry> previousEntry = histMap.lowerEntry(lastEntry.getKey());

        WALPointer lastWALPointer = lastEntry.getValue().checkpointMark();

        long lastIdx = 0;

        long prevIdx = 0;

        if (lastWALPointer instanceof FileWALPointer) {
            lastIdx = ((FileWALPointer)lastWALPointer).index();

            if (previousEntry != null)
                prevIdx = ((FileWALPointer)previousEntry.getValue().checkpointMark()).index();
        }

        tup.set1(prevIdx);
        tup.set2(lastIdx - 1);

        return tup;
    }

    /**
     * Search the earliest WAL pointer for particular group, matching by counter for partitions.
     *
     * @param grpId Group id.
     * @param partsCounter Partition mapped to update counter.
     * @param margin Margin pointer.
     * @return Earliest WAL pointer for group specified.
     */
    @Nullable public FileWALPointer searchEarliestWalPointer(
        int grpId,
        Map<Integer, Long> partsCounter,
        long margin
    ) throws IgniteCheckedException {
        if (F.isEmpty(partsCounter))
            return null;

        Map<Integer, Long> modifiedPartsCounter = new HashMap<>(partsCounter);

        FileWALPointer minPtr = null;

        LinkedList<WalPointerCandidate> historyPointerCandidate = new LinkedList<>();

        for (Long cpTs : checkpoints(true)) {
            CheckpointEntry cpEntry = entry(cpTs);

            minPtr = getMinimalPointer(partsCounter, margin, minPtr, historyPointerCandidate, cpEntry);

            Iterator<Map.Entry<Integer, Long>> iter = modifiedPartsCounter.entrySet().iterator();

            FileWALPointer ptr = (FileWALPointer)cpEntry.checkpointMark();

            if (!wal.reserved(ptr)) {
                throw new IgniteCheckedException("WAL pointer appropriate to the checkpoint was not reserved " +
                    "[cp=(" + cpEntry.checkpointId() + ", " + U.format(cpEntry.timestamp())
                    + "), ptr=" + ptr + ']');
            }

            while (iter.hasNext()) {
                Map.Entry<Integer, Long> entry = iter.next();

                Long foundCntr = cpEntry.partitionCounter(wal, grpId, entry.getKey());

                if (foundCntr != null && foundCntr <= entry.getValue()) {
                    iter.remove();

                    if (ptr == null) {
                        throw new IgniteCheckedException("Could not find start pointer for partition [part="
                            + entry.getKey() + ", partCntrSince=" + entry.getValue() + "]");
                    }

                    if (foundCntr + margin > entry.getValue()) {
                        historyPointerCandidate.add(new WalPointerCandidate(grpId, entry.getKey(), entry.getValue(), ptr,
                            foundCntr));

                        continue;
                    }

                    partsCounter.put(entry.getKey(), entry.getValue() - margin);

                    if (minPtr == null || ptr.compareTo(minPtr) < 0)
                        minPtr = ptr;
                }
            }

            if (F.isEmpty(modifiedPartsCounter))
                break;
        }

        if (!F.isEmpty(modifiedPartsCounter)) {
            Map.Entry<Integer, Long> entry = modifiedPartsCounter.entrySet().iterator().next();

            throw new IgniteCheckedException("Could not find start pointer for partition [part="
                + entry.getKey() + ", partCntrSince=" + entry.getValue() + "]");
        }

        minPtr = getMinimalPointer(partsCounter, margin, minPtr, historyPointerCandidate, null);

        return minPtr;
    }

    /**
     * Finds a minimal WAL pointer.
     *
     * @param partsCounter Partition mapped to update counter.
     * @param margin Margin pointer.
     * @param minPtr Minimal WAL pointer which was determined before.
     * @param historyPointerCandidate Collection of candidates for a historical WAL pointer.
     * @param cpEntry Checkpoint entry.
     * @return Minimal WAL pointer.
     */
    private FileWALPointer getMinimalPointer(
        Map<Integer, Long> partsCounter,
        long margin,
        FileWALPointer minPtr,
        LinkedList<WalPointerCandidate> historyPointerCandidate,
        CheckpointEntry cpEntry
    ) {
        while (!F.isEmpty(historyPointerCandidate)) {
            FileWALPointer ptr = historyPointerCandidate.poll()
                .choose(cpEntry, margin, partsCounter);

            if (minPtr == null || ptr.compareTo(minPtr) < 0)
                minPtr = ptr;
        }

        return minPtr;
    }

    /**
     * The class is used for get a pointer with a specific margin.
     * This stores the nearest pointer which covering a partition counter.
     * It is able to choose between other pointer and this.
     */
    private class WalPointerCandidate {
        /** Group id. */
        private final int grpId;

        /** Partition id. */
        private final int part;

        /** Partition counter. */
        private final long partContr;

        /** WAL pointer. */
        private final FileWALPointer walPntr;

        /** Partition counter at the moment of WAL pointer. */
        private final long walPntrCntr;

        /**
         * @param grpId Group id.
         * @param part Partition id.
         * @param partContr Partition counter.
         * @param walPntr WAL pointer.
         * @param walPntrCntr Counter of WAL pointer.
         */
        public WalPointerCandidate(int grpId, int part, long partContr, FileWALPointer walPntr, long walPntrCntr) {
            this.grpId = grpId;
            this.part = part;
            this.partContr = partContr;
            this.walPntr = walPntr;
            this.walPntrCntr = walPntrCntr;
        }

        /**
         * Make a choice between stored WAL pointer and other, getting from checkpoint, with a specific margin.
         * Updates counter in collection from parameters.
         *
         * @param cpEntry Checkpoint entry.
         * @param margin Margin.
         * @param partCntsForUpdate Collection of partition id by counter.
         * @return Chosen WAL pointer.
         */
        public FileWALPointer choose(
            CheckpointEntry cpEntry,
            long margin,
            Map<Integer, Long> partCntsForUpdate
        ) {
            Long foundCntr = null;

            try {
                foundCntr = cpEntry == null ? null : cpEntry.partitionCounter(wal, grpId, part);
            }
            catch (IgniteCheckedException e) {
                log.warning("Checkpoint cannot be chosen because counter is unavailable [grpId=" + grpId
                    + ", part=" + part
                    + ", cp=(" + cpEntry.checkpointId() + ", " + U.format(cpEntry.timestamp()) + ")]", e);
            }

            if (foundCntr == null || foundCntr == walPntrCntr) {
                partCntsForUpdate.put(part, walPntrCntr);

                return walPntr;
            }

            partCntsForUpdate.put(part, Math.max(foundCntr, partContr - margin));

            return (FileWALPointer)cpEntry.checkpointMark();
        }
    }

    /**
     * Tries to search for a WAL pointer for the given partition counter start.
     *
     * @param searchCntrMap Search map contains (Group Id, partition, counter).
     * @return Map of group-partition on checkpoint entry or empty map if nothing found.
     */
    public Map<GroupPartitionId, CheckpointEntry> searchCheckpointEntry(
        Map<T2<Integer, Integer>, Long> searchCntrMap
    ) {
        if (F.isEmpty(searchCntrMap))
            return Collections.emptyMap();

        Map<T2<Integer, Integer>, Long> modifiedSearchMap = new HashMap<>(searchCntrMap);

        Map<GroupPartitionId, CheckpointEntry> res = new HashMap<>();

        for (Long cpTs : checkpoints(true)) {
            try {
                CheckpointEntry cpEntry = entry(cpTs);

                Iterator<Map.Entry<T2<Integer, Integer>, Long>> iter = modifiedSearchMap.entrySet().iterator();

                while (iter.hasNext()) {
                    Map.Entry<T2<Integer, Integer>, Long> entry = iter.next();

                    Long foundCntr = cpEntry.partitionCounter(wal, entry.getKey().get1(), entry.getKey().get2());

                    if (foundCntr != null && foundCntr <= entry.getValue()) {
                        iter.remove();

                        res.put(new GroupPartitionId(entry.getKey().get1(), entry.getKey().get2()), cpEntry);
                    }
                }

                if (F.isEmpty(modifiedSearchMap))
                    return res;
            }
            catch (IgniteCheckedException e) {
                log.warning("Checkpoint data is unavailable in WAL [cpTs=" + U.format(cpTs) + ']', e);

                break;
            }
        }

        if (!F.isEmpty(modifiedSearchMap))
            return Collections.emptyMap();

        return res;
    }

    /**
     * Tries to search for a WAL pointer for the given partition counter start.
     *
     * @param grpId Cache group ID.
     * @param part Partition ID.
     * @param partCntrSince Partition counter or {@code null} to search for minimal counter.
     * @return Checkpoint entry or {@code null} if failed to search.
     */
    @Nullable public CheckpointEntry searchCheckpointEntry(int grpId, int part, long partCntrSince) {
        for (Long cpTs : checkpoints(true)) {
            try {
                CheckpointEntry entry = entry(cpTs);

                Long foundCntr = entry.partitionCounter(wal, grpId, part);

                if (foundCntr != null && foundCntr <= partCntrSince)
                    return entry;
            }
            catch (IgniteCheckedException e) {
                log.warning("Checkpoint data is unavailable in WAL [grpId=" + grpId
                    + ", part=" + part
                    + ", cntr=" + partCntrSince
                    + ", cpTs=" + U.format(cpTs) + ']', e);

                break;
            }
        }

        return null;
    }

    /**
     * Finds and reserves earliest valid checkpoint for each of given groups and partitions.
     *
     * @param groupsAndPartitions Groups and partitions to find and reserve earliest valid checkpoint.
     * @return Checkpoint history reult: Map (groupId, Reason (the reason why reservation cannot be made deeper): Map
     * (partitionId, earliest valid checkpoint to history search)) and reserved checkpoint.
     */
    public CheckpointHistoryResult searchAndReserveCheckpoints(
        final Map<Integer, Set<Integer>> groupsAndPartitions
    ) {
        if (F.isEmpty(groupsAndPartitions) || reservationDisabled)
            return new CheckpointHistoryResult(Collections.emptyMap(), null);

        final Map<Integer, T2<ReservationReason, Map<Integer, CheckpointEntry>>> res = new HashMap<>();

        CheckpointEntry oldestCpForReservation = null;

        synchronized (earliestCpGrps) {
            CheckpointEntry oldestHistCpEntry = firstCheckpoint();

            for (Map.Entry<Integer, Set<Integer>> e0 : groupsAndPartitions.entrySet()) {
                CheckpointEntry oldestGrpCpEntry = null;

                Integer grpId = e0.getKey();

                CacheGroupContext grpCtx = cacheGrpCtxSupplier.get(grpId);

                if (grpCtx == null || !earliestCpGrps.contains(grpId))
                    continue;

                for (Integer partId : e0.getValue()) {
                    GridDhtLocalPartition localPart = grpCtx.topology().localPartition(partId);

                    if (localPart == null)
                        continue;

                    CheckpointEntry cp = histMap.get(localPart.earliestCpTs());

                    if (cp == null)
                        continue;

                    if (oldestCpForReservation == null || oldestCpForReservation.timestamp() > cp.timestamp())
                        oldestCpForReservation = cp;

                    if (oldestGrpCpEntry == null || oldestGrpCpEntry.timestamp() > cp.timestamp())
                        oldestGrpCpEntry = cp;

                    res.computeIfAbsent(
                        grpId,
                        partCpMap -> new T2<>(ReservationReason.NO_MORE_HISTORY, new HashMap<>())
                    ).get2().put(partId, cp);
                }

                if (oldestGrpCpEntry == null || oldestGrpCpEntry != oldestHistCpEntry) {
                    res.computeIfAbsent(grpId, (partCpMap) ->
                            new T2<>(ReservationReason.CHECKPOINT_NOT_APPLICABLE, null))
                        .set1(ReservationReason.CHECKPOINT_NOT_APPLICABLE);
                }
            }
        }

        if (oldestCpForReservation != null) {
            if (!wal.reserve(oldestCpForReservation.checkpointMark())) {
                log.warning("Could not reserve cp " + oldestCpForReservation.checkpointMark());

                for (Map.Entry<Integer, T2<ReservationReason, Map<Integer, CheckpointEntry>>> entry : res.entrySet())
                    entry.setValue(new T2<>(ReservationReason.WAL_RESERVATION_ERROR, null));

                oldestCpForReservation = null;
            }
        }

        return new CheckpointHistoryResult(res, oldestCpForReservation);
    }

    /**
     * Checkpoint is not applicable when:
     * 1) WAL was disabled somewhere after given checkpoint.
     * 2) Checkpoint doesn't contain specified {@code grpId}.
     * Checkpoint is not applicable when: 1) WAL was disabled somewhere after given checkpoint. 2) Checkpoint doesn't
     * contain specified {@code grpId}.
     *
     * @param grpId Group ID.
     * @param cp Checkpoint.
     */
    public boolean isCheckpointApplicableForGroup(int grpId, CheckpointEntry cp) throws IgniteCheckedException {
        return !checkpointInapplicable.test(cp.timestamp(), grpId) && cp.groupState(wal).containsKey(grpId);
    }

    /**
     * Creates a snapshot of map that stores the earliest checkpoint for each partition from a particular group.
     * Guarded by checkpoint read lock.
     *
     * @return Snapshot of a map.
     */
    public EarliestCheckpointMapSnapshot earliestCheckpointsMapSnapshot() {
        Map<UUID, Map<Integer, GroupStateSnapshot>> earliestCp = new HashMap<>();

        synchronized (earliestCpGrps) {
            for (Integer grpId : earliestCpGrps) {
                CacheGroupContext grpCtx = cacheGrpCtxSupplier.get(grpId);

                if (grpCtx == null)
                    continue;

                for (GridDhtLocalPartition localPart : grpCtx.topology().localPartitions()) {
                    long earliestCpTs = localPart.earliestCpTs();

                    if (earliestCpTs == 0)
                        continue;

                    CheckpointEntry cp = histMap.get(earliestCpTs);

                    if (cp == null || cp.groupStates() == null || earliestCp.containsKey(cp.checkpointId()))
                        continue;

                    earliestCp.put(cp.checkpointId(), createSnapshot(cp.groupStates()));
                }
            }
        }

        Set<UUID> histCpIds = histMap.values().stream()
            .map(CheckpointEntry::checkpointId)
            .collect(toSet());

        return new EarliestCheckpointMapSnapshot(histCpIds, earliestCp);
    }

    /**
     * Clear all cached data.
     */
    void clear() {
        histMap.clear();
        earliestCpGrps.clear();
    }

    /** */
    private static Map<Integer, GroupStateSnapshot> createSnapshot(Map<Integer, GroupState> stateByGrpId) {
        Map<Integer, GroupStateSnapshot> snapshot = new HashMap<>();

        for (Map.Entry<Integer, GroupState> e : stateByGrpId.entrySet()) {
            GroupState grpState = e.getValue();

            GroupStateSnapshot grpStateSnapshot = new GroupStateSnapshot(
                grpState.partitionIds(),
                grpState.partitionCounters(),
                grpState.size()
            );

            snapshot.put(e.getKey(), grpStateSnapshot);
        }

        return snapshot;
    }

    /** */
    private void clearEarliestCpTsOfGrp(int grpId) {
        CacheGroupContext grpCtx = cacheGrpCtxSupplier.get(grpId);

        if (grpCtx != null)
            grpCtx.topology().localPartitions().forEach(localPart -> localPart.earliestCpTs(0));
    }

    /** */
    private void setLocalPartitionEarliestCpTs(int grpId, int partId, long earliestCpTs) {
        CacheGroupContext grpCtx = cacheGrpCtxSupplier.get(grpId);

        if (grpCtx == null)
            return;

        GridDhtLocalPartition locPart = grpCtx.topology().localPartition(partId);

        if (locPart == null)
            return;

        earliestCpGrps.add(grpId);

        if (locPart.earliestCpTs() == 0)
            locPart.earliestCpTs(earliestCpTs);
    }

    /** */
    private void applyEarliestCpSnapshot() {
        EarliestCheckpointMapSnapshot snapshot = earliestCpSnapshot.getAndSet(null);

        if (snapshot == null)
            return;

        for (Long timestamp : checkpoints(false)) {
            try {
                CheckpointEntry entry = entry(timestamp);

                UUID checkpointId = entry.checkpointId();

                Map<Integer, GroupState> groupStateMap = snapshot.groupState(checkpointId);

                // Ignore checkpoint that was present at the time of the snapshot and whose group
                // states map was not persisted (that means this checkpoint wasn't a part of earliestCp map)
                if (snapshot.checkpointWasPresent(checkpointId) && groupStateMap == null)
                    continue;

                if (groupStateMap != null)
                    entry.fillStore(groupStateMap);

                updateEarliestCpMap(entry, groupStateMap);
            }
            catch (IgniteCheckedException e) {
                U.warn(log, "Failed to process checkpoint, happened at " + U.format(timestamp) + '.', e);
            }
        }
    }

    /**
     * Creates a snapshot of the earliest checkpoint to recover on {@link #start()}, in memory without saving to disk.
     */
    void createInMemoryEarliestCpSnapshot() {
        EarliestCheckpointMapSnapshot snapshot = earliestCheckpointsMapSnapshot();

        earliestCpSnapshot.set(snapshot);
    }
}

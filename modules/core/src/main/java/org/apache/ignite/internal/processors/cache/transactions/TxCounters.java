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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.util.GridPartitionStateMap;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 * Values which should be tracked during transaction execution and applied on commit.
 */
public class TxCounters {
    /** Initial capacity. */
    private static final int INITIAL_CAPACITY = 8;

    /** Maximum capacity. */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    /** Magic hash mixer. */
    private static final int MAGIC_HASH_MIXER = 0x9E3779B9;

    /** Array load percentage before resize. */
    private static final float SCALE_LOAD_FACTOR = 0.7F;

    /** Parent tx. */
    private final IgniteTxAdapter tx;

    /** Size changes for cache partitions made by transaction */
    private final CountersMap sizeDeltas = new CountersMap();

    /** Per-partition update counter accumulator. */
    private final CountersMap cntrsMap = new CountersMap();

    /** Final update counters for cache partitions in the end of transaction */
    private IntHashMap<PartitionUpdateCountersMessage> updCntrs;

    /** Counter tracking number of entries locked by tx. */
    private int lockCntr;

    public TxCounters(IgniteTxAdapter tx) {
        this.tx = tx;
    }

    /**
     * @param cacheId Cache id.
     * @param part Partition number.
     */
    public void incrementPartitionSize(int cacheId, int part) {
        sizeDeltas.increment(cacheId, part);
    }

    /**
     * @param cacheId Cache id.
     * @param part Partition number.
     */
    public void decrementPartitionSize(int cacheId, int part) {
        sizeDeltas.decrement(cacheId, part);
    }

    /**
     * @param cacheId Cache id.
     * @param part Partition number.
     */
    public void incrementUpdateCounter(int cacheId, int part) {
        cntrsMap.increment(cacheId, fixPart(part));
    }

    /**
     * @param cacheId Cache id.
     * @param part Partition number.
     */
    public void decrementUpdateCounter(int cacheId, int part) {
        cntrsMap.decrement(cacheId, fixPart(part));
    }

    /**
     * Increments lock counter.
     */
    public synchronized void incrementLockCounter() {
        lockCntr++;
    }

    /**
     * @return Current value of lock counter.
     */
    public synchronized int lockCounter() {
        return lockCntr;
    }

    /**
     * @param updCntrs Final update counters.
     */
    public void updateCounters(Collection<PartitionUpdateCountersMessage> updCntrs) {
        this.updCntrs = new IntHashMap<>(updCntrs.size());

        for (PartitionUpdateCountersMessage cntr : updCntrs)
            this.updCntrs.put(cntr.cacheId(), cntr);
    }

    /**
     * @return Final update counters.
     */
    @Nullable public Collection<PartitionUpdateCountersMessage> updateCounters() {
        return updCntrs == null ? null : updCntrs.values();
    }

    /**
     * @return Final update counters for given backup node.
     */
    @Nullable public List<PartitionUpdateCountersMessage> updateCountersForBackupNode(ClusterNode node) {
        List<PartitionUpdateCountersMessage> res = null;

        if (updCntrs != null && !updCntrs.isEmpty()) {
            for (PartitionUpdateCountersMessage partCntrs : updCntrs.values()) {
                GridPartitionStateMap stateMap = partCntrs.context().topology().partitions(node.id()).map();

                PartitionUpdateCountersMessage resCntrs = null;

                for (int i = 0; i < partCntrs.size(); i++) {
                    int part = partCntrs.partition(i);

                    GridDhtPartitionState state = stateMap.get(part);

                    if (state == GridDhtPartitionState.OWNING || state == GridDhtPartitionState.MOVING) {
                        if (res == null)
                            res = new ArrayList<>(updCntrs.size());

                        if (resCntrs == null)
                            res.add(resCntrs = new PartitionUpdateCountersMessage(partCntrs.cacheId(), partCntrs.size()));

                        resCntrs.add(part, partCntrs.initialCounter(i), partCntrs.updatesCount(i));
                    }
                }
            }
        }

        return res;
    }

    /**
     * @param cacheId Cache id.
     * @param partId Partition id.
     *
     * @return Counter or {@code null} if cache partition has not updates.
     */
    public Long generateNextCounter(int cacheId, int partId) {
        PartitionUpdateCountersMessage msg = updCntrs.get(cacheId);

        if (msg == null)
            return null;

        return msg.nextCounter(partId);
    }

    /**
     * Makes cache sizes changes accumulated during transaction visible outside of transaction.
     */
    public void applySizeDeltas() {
        if (sizeDeltas.size > 0) {
            for (Entry entry : sizeDeltas.entries) {
                if (entry == null || entry.size == 0)
                    continue;

                GridDhtPartitionTopology top = tx.cctx.cacheContext(entry.cacheId).topology();

                // Need to reserve on backups only
                boolean reserve = tx.dht() && tx.remote();

                for (long partEntry : entry.data) {
                    if (partEntry == 0)
                        continue;

                    int p = fixPart(part(partEntry));

                    boolean invalid = false;

                    try {
                        GridDhtLocalPartition part = top.localPartition(p);

                        if (!reserve || part != null && part.reserve()) {
                            assert part != null;

                            try {
                                if (part.state() != GridDhtPartitionState.RENTING)
                                    part.dataStore().updateSize(entry.cacheId, counter(partEntry));
                                else
                                    invalid = true;
                            }
                            finally {
                                if (reserve)
                                    part.release();
                            }
                        }
                        else
                            invalid = true;
                    }
                    catch (GridDhtInvalidPartitionException e1) {
                        invalid = true;
                    }

                    if (invalid) {
                        assert reserve;

                        if (IgniteTxAdapter.log.isDebugEnabled())
                            IgniteTxAdapter.log.debug("Trying to apply size delta for invalid partition: " +
                                "[cacheId=" + entry.cacheId + ", part=" + p + "]");
                    }
                }

            }
        }
    }

    /**
     * Calculates partition update counters for current transaction. Each partition will be supplied with
     * pair (init, delta) values, where init - initial update counter, and delta - updates count made
     * by current transaction for a given partition.
     */
    public void calculateUpdateCounters() throws IgniteTxRollbackCheckedException {
        if (updCntrs == null) {
            updCntrs = new IntHashMap<>(cntrsMap.size);

            if (cntrsMap.size > 0) {
                for (Entry entry : cntrsMap.entries) {
                    if (entry == null || entry.size == 0)
                        continue;

                    GridCacheContext<?, ?> cctx = tx.cctx.cacheContext(entry.cacheId);

                    PartitionUpdateCountersMessage msg = new PartitionUpdateCountersMessage(cctx, entry.size);

                    updCntrs.put(msg.cacheId(), msg);

                    GridDhtPartitionTopology top = cctx.topology();

                    for (long partEntry : entry.data) {
                        if (partEntry == 0)
                            continue;

                        int p = fixPart(part(partEntry));

                        GridDhtLocalPartition part = top.localPartition(p);

                        checkPartition(top, part);

                        int cntr = counter(partEntry);

                        msg.add(p, part.getAndIncrementUpdateCounter(cntr), cntr);
                    }
                }
            }
        }
    }

    private void checkPartition(GridDhtPartitionTopology top, GridDhtLocalPartition part) throws IgniteTxRollbackCheckedException {
        // Verify primary tx mapping.
        // LOST state is possible if tx is started over LOST partition.
        boolean valid = part != null &&
            (part.state() == OWNING || part.state() == LOST) &&
            part.primary(top.readyTopologyVersion());

        if (!valid) {
            // Local node is no longer primary for the partition, need to rollback a transaction.
            if (part != null && !part.primary(top.readyTopologyVersion())) {
                IgniteTxAdapter.log.warning("Failed to prepare a transaction on outdated topology, rolling back " +
                    "[tx=" + CU.txString(tx) +
                    ", readyTopVer=" + top.readyTopologyVersion() +
                    ", lostParts=" + top.lostPartitions() +
                    ", part=" + part.toString() + ']');

                throw new IgniteTxRollbackCheckedException("Failed to prepare a transaction on outdated " +
                    "topology, please try again [timeout=" + tx.timeout() + ", tx=" + CU.txString(tx) + ']');
            }

            // Trigger error.
            throw new AssertionError("Invalid primary mapping [tx=" + CU.txString(tx) +
                ", readyTopVer=" + top.readyTopologyVersion() +
                ", lostParts=" + top.lostPartitions() +
                ", part=" + (part == null ? "NULL" : part.toString()) + ']');
        }
    }

    /** */
    private static final class CountersMap {
        /** Scale threshold. */
        private int scaleThreshold = (int)(INITIAL_CAPACITY * SCALE_LOAD_FACTOR);

        /** Entries. */
        private Entry[] entries = new Entry[INITIAL_CAPACITY];

        /** Count of elements in Map. */
        private int size;

        /** */
        private void increment(int cacheId, int partId) {
            entry(cacheId).increment(partId);
        }

        /** */
        private void decrement(int cacheId, int partId) {
            entry(cacheId).decrement(partId);
        }

        /** */
        private void applyDelta(int cacheId, int partId, int delta) {
            entry(cacheId).applyDelta(partId, delta);
        }

        /** */
        private Entry entry(int cacheId) {
            int tabLen = entries.length;

            int idx = index(cacheId, tabLen);

            for (int keyDist = 0; keyDist < tabLen; keyDist++) {
                int curIdx = (idx + keyDist) & (tabLen - 1);

                Entry entry = entries[curIdx];

                if (entry == null)
                    return newEntry(cacheId);

                if (entry.cacheId == cacheId)
                    return entry;

                if (keyDist > distance(curIdx, entry.cacheId, tabLen))
                    return newEntry(cacheId);
            }

            return newEntry(cacheId);
        }

        /** */
        private Entry newEntry(int cacheId) {
            Entry entry = new Entry(cacheId);

            put0(entry);

            return entry;
        }

        /** */
        private void put0(Entry entry) {
            if (size >= scaleThreshold)
                resize();

            int tabLen = entries.length;

            Entry savedEntry = entry;

            int startKey = savedEntry.cacheId;

            for (int i = 0; i < tabLen; i++) {
                int idx = (index(startKey, tabLen) + i) & (tabLen - 1);

                Entry curEntry = entries[idx];

                if (curEntry == null) {
                    entries[idx] = savedEntry;

                    size++;

                    return;
                }
                else if (curEntry.cacheId == savedEntry.cacheId) {
                    entries[idx] = savedEntry;

                    return;
                }

                int curDist = distance(idx, curEntry.cacheId, tabLen);
                int savedDist = distance(idx, savedEntry.cacheId, tabLen);

                if (curDist < savedDist) {
                    entries[idx] = savedEntry;

                    savedEntry = curEntry;
                }
            }

            throw new AssertionError("Unreachable state exception. Insertion position not found. " +
                "Entry: " + entry + " map state: " + toString());
        }

        /** */
        private void resize() {
            int tabLen = entries.length;

            if (MAXIMUM_CAPACITY == tabLen)
                throw new IllegalStateException("Maximum capacity: " + MAXIMUM_CAPACITY + " is reached.");

            Entry[] oldEntries = entries;

            entries = new Entry[tabLen << 1];

            scaleThreshold = (int)(tabLen * SCALE_LOAD_FACTOR);

            size = 0;

            for (Entry entry : oldEntries)
                if (entry != null)
                    put0(entry);
        }
    }

    /** */
    private static final class Entry {
        /** */
        private final int cacheId;

        /** Scale threshold. */
        private int scaleThreshold;

        /** Count of elements in Map. */
        private int size;

        /** data array. */
        private long[] data;

        /** Default constructor. */
        private Entry(int cacheId) {
            scaleThreshold = (int)(INITIAL_CAPACITY * SCALE_LOAD_FACTOR);

            data = new long[INITIAL_CAPACITY];

            this.cacheId = cacheId;
        }

        /** */
        private void increment(int part) {
            applyDelta(part, 1);
        }

        /** */
        private void decrement(int part) {
            applyDelta(part, -1);
        }

        /** */
        private void applyDelta(int part, int delta) {
            int idx = find(part);

            if (idx < 0)
                put0(entry(part, delta));
            else
                data[idx] = entry(part, counter(data[idx]) + delta);
        }

        private void put0(long entry) {
            if (size >= scaleThreshold)
                resize();

            int tabLen = data.length;

            long savedEntry = entry;

            int startKey = part(savedEntry);

            for (int i = 0; i < tabLen; i++) {
                int idx = (index(startKey, tabLen) + i) & (tabLen - 1);

                long curEntry = data[idx];

                if (curEntry == 0) {
                    data[idx] = savedEntry;

                    size++;

                    return;
                }
                else if ((curEntry ^ savedEntry) >>> 32 == 0) {
                    data[idx] = savedEntry;

                    return;
                }

                int curDist = distance(idx, part(curEntry), tabLen);
                int savedDist = distance(idx, part(savedEntry), tabLen);

                if (curDist < savedDist) {
                    data[idx] = savedEntry;

                    savedEntry = curEntry;
                }
            }

            throw new AssertionError("Unreachable state exception. Insertion position not found. " +
                "Entry: " + entry + " map state: " + toString());
        }

        private int find(int part) {
            int idx = index(part, data.length);

            for (int dist = 0; dist < data.length; dist++) {
                int curIdx = (idx + dist) & (data.length - 1);

                long entry = data[curIdx];

                if (entry == 0)
                    return -1;
                else if (part(entry) == part)
                    return curIdx;

                int entryDist = distance(curIdx, part(entry), data.length);

                if (dist > entryDist)
                    return -1;
            }

            return -1;
        }

        private void resize() {
            int tabLen = data.length;

            if (MAXIMUM_CAPACITY == tabLen)
                throw new IllegalStateException("Maximum capacity: " + MAXIMUM_CAPACITY + " is reached.");

            long[] oldEntries = data;

            data = new long[tabLen << 1];

            scaleThreshold = (int)(tabLen * SCALE_LOAD_FACTOR);

            size = 0;

            for (long entry : oldEntries)
                if (entry != 0)
                    put0(entry);
        }
    }

    /** */
    private static int distance(int curIdx, int key, int tabLen) {
        int keyIdx = index(key, tabLen);

        return curIdx >= keyIdx ? curIdx - keyIdx : tabLen - keyIdx + curIdx;
    }

    /** */
    private static int index(int key, int tabLen) {
        return (tabLen - 1) & ((key ^ (key >>> 16)) * MAGIC_HASH_MIXER);
    }

    /** */
    private static int part(long entry) {
        return (int) (entry >>> 32);
    }

    /** */
    private static int counter(long entry) {
        return (int) entry;
    }

    /** */
    private static long entry(int part, int counter) {
        return (long)part << 32 | counter;
    }

    /** */
    private static int fixPart(int part) {
        return ~part;
    }
}

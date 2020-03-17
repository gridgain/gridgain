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

package org.apache.ignite.internal.processors.cache.verify;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for idle verify command.
 */
public class IdleVerifyUtility {
    /** Cluster not idle message. */
    public static final String IDLE_DATA_ALTERATION_MSG =
        "Cluster not idle. Modifications found in caches or groups: ";

    /**
     * See {@link IdleVerifyUtility#checkPartitionsPageCrcSum(FilePageStore, CacheGroupContext, int, byte)}.
     */
    public static void checkPartitionsPageCrcSum(
        @Nullable FilePageStoreManager pageStoreMgr,
        CacheGroupContext grpCtx,
        int partId,
        byte pageType
    ) throws IgniteCheckedException, GridNotIdleException {
        if (!grpCtx.persistenceEnabled() || pageStoreMgr == null)
            return;

        FilePageStore pageStore = (FilePageStore)pageStoreMgr.getStore(grpCtx.groupId(), partId);

        checkPartitionsPageCrcSum(pageStore, grpCtx, partId, pageType);
    }

    /**
     * Checks CRC sum of pages with {@code pageType} page type stored in partiion with {@code partId} id and assosiated
     * with cache group. <br/> Method could be invoked only on idle cluster!
     *
     * @param pageStore Page store.
     * @param grpCtx Passed cache group context.
     * @param partId Partition id.
     * @param pageType Page type. Possible types {@link PageIdAllocator#FLAG_DATA}, {@link PageIdAllocator#FLAG_IDX}.
     * @throws IgniteCheckedException If reading page failed.
     * @throws GridNotIdleException If cluster not idle.
     */
    public static void checkPartitionsPageCrcSum(
        FilePageStore pageStore,
        CacheGroupContext grpCtx,
        int partId,
        byte pageType
    ) throws IgniteCheckedException, GridNotIdleException {
        assert pageType == PageIdAllocator.FLAG_DATA || pageType == PageIdAllocator.FLAG_IDX : pageType;

        long pageId = PageIdUtils.pageId(partId, pageType, 0);

        ByteBuffer buf = ByteBuffer.allocateDirect(grpCtx.dataRegion().pageMemory().pageSize());

        buf.order(ByteOrder.nativeOrder());

        for (int pageNo = 0; pageNo < pageStore.pages(); pageId++, pageNo++) {
            buf.clear();

            pageStore.read(pageId, buf, true);
        }
    }

    /**
     * Gather updateCounters info.
     *
     * @param ign Ignite instance.
     * @param grpIds Group Id`s.
     * @return Current group id`s and distribution partitions with update counters per group.
     */
    public static T2<Set<Integer>, Map<Integer, Set<Map.Entry<Integer, Long>>>> updCountersSnapshot(
        IgniteEx ign,
        Set<Integer> grpIds
    ) {
        Map<Integer, Set<Map.Entry<Integer, Long>>> partsWithCountersPerGrp = new HashMap<>();

        for (Integer grpId : grpIds) {
            CacheGroupContext grpCtx = ign.context().cache().cacheGroup(grpId);

            if (grpCtx == null)
                throw new GridNotIdleException("Possibly rebalance in progress? Group not found: " + grpId);

            GridDhtPartitionTopology top = grpCtx.topology();

            Set<Map.Entry<Integer, Long>> partsWithCounters =
                partsWithCountersPerGrp.computeIfAbsent(grpId, k -> new HashSet<>());

            for (GridDhtLocalPartition part : top.currentLocalPartitions()) {
                if (part.state() != GridDhtPartitionState.OWNING)
                    continue;

                partsWithCounters.add(new AbstractMap.SimpleEntry<>(part.id(), part.updateCounter()));
            }
        }

        return new T2<>(grpIds, partsWithCountersPerGrp);
    }

    /**
     * Compares two sets with partitions and upd counters per group distribution.
     *
     * @param ign Ignite instance.
     * @param cntrsIn Group id`s with counters per partitions per groups distribution.
     * @return Diff with grpId info between two sets.
     */
    public static List<Integer> compareUpdCounters(
        IgniteEx ign,
        T2<Set<Integer>, Map<Integer, Set<Map.Entry<Integer, Long>>>> cntrsIn
    ) {
        T2<Set<Integer>, Map<Integer, Set<Map.Entry<Integer, Long>>>> curCntrs =
            updCountersSnapshot(ign, cntrsIn.getKey());

        List<Integer> diff = new ArrayList<>();

        for (Integer grp : cntrsIn.getValue().keySet()) {
            Set<Map.Entry<Integer, Long>> partsWithCntrsCur = curCntrs.getValue().get(grp);

            if (partsWithCntrsCur == null)
                throw new GridNotIdleException("Possibly rebalance in progress? Group not found: " + grp);

            Set<Map.Entry<Integer, Long>> partsWithCntrsIn = cntrsIn.getValue().get(grp);

            if (!partsWithCntrsIn.equals(partsWithCntrsCur))
                diff.add(grp);
        }

        return diff;
    }

    /**
     * Idle checker.
     */
    public static class IdleChecker implements Runnable {
        /** */
        private final IgniteEx ig;

        /** Group id`s snapshot with partitions and counters distrubution. */
        private final T2<Set<Integer>, Map<Integer, Set<Map.Entry<Integer, Long>>>> partsWithCntrsPerGrp;

        /** */
        public IdleChecker(IgniteEx ig, T2<Set<Integer>, Map<Integer, Set<Map.Entry<Integer, Long>>>> partsWithCntrsPerGrp) {
            this.ig = ig;
            this.partsWithCntrsPerGrp = partsWithCntrsPerGrp;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            List<Integer> diff = compareUpdCounters(ig, partsWithCntrsPerGrp);

            SB sb = new SB();

            if (!diff.isEmpty()) {
                for (int grpId : diff) {
                    if (sb.length() != 0)
                        sb.a(", ");
                    else
                        sb.a("\"");

                    DynamicCacheDescriptor desc = ig.context().cache().cacheDescriptor(grpId);

                    CacheGroupContext grpCtx = ig.context().cache().cacheGroup(desc == null ? grpId : desc.groupId());

                    sb.a(grpCtx.cacheOrGroupName());
                }

                sb.a("\"");

                throw new GridNotIdleException(IDLE_DATA_ALTERATION_MSG + "[" + sb.toString() + "]");
            }
        }
    }

    /** */
    private IdleVerifyUtility() {
        /* No-op. */
    }
}

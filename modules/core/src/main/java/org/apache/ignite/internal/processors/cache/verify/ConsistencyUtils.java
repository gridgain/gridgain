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

package org.apache.ignite.internal.processors.cache.verify;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.CacheQueryObjectValueContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Contains method for debugging data consistency issues.
 */
public class ConsistencyUtils {
    /**
     * Prints key divergence details for the first inconsistent partition.
     *
     * @param res Verification result.
     * @param log Logger.
     */
    public static void printDivergenceDetailsForKey(IdleVerifyResultV2 res, IgniteLogger log) throws IgniteCheckedException {
        if (F.isEmpty(res.hashConflicts()))
            return;

        Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> e0 = res.hashConflicts().entrySet().iterator().next();

        List<PartitionHashRecordV2> list = e0.getValue();
        PartitionKeyV2 first = e0.getKey();

        list.sort(new Comparator<PartitionHashRecordV2>() {
            @Override public int compare(PartitionHashRecordV2 o1, PartitionHashRecordV2 o2) {
                return o1.consistentId().toString().compareTo(o2.consistentId().toString());
            }
        });

        final IgniteEx g0 = grid(list.get(0).consistentId());
        final IgniteEx g1 = list.stream().skip(1).filter(r -> r.partitionHash() != list.get(0).partitionHash()).
            map(l -> grid(l.consistentId())).findFirst().
            orElseThrow(() -> new IgniteCheckedException("Failed to find a node by consistent id"));

        int part = first.partitionId();

        CacheGroupContext grpCtx0 = g0.context().cache().cacheGroup(first.groupId());
        List<CacheDataRow> dataRows0 = rows(grpCtx0, part);

        CacheGroupContext grpCtx1 = g1.context().cache().cacheGroup(first.groupId());
        List<CacheDataRow> dataRows1 = rows(grpCtx1, part);

        // Find any divergent key.
        CacheDataRow testRow;

        CacheQueryObjectValueContext ctx =
            new CacheQueryObjectValueContext(grpCtx0.cacheObjectContext().kernalContext());

        List<CacheDataRow> diff = diff(dataRows0, dataRows1, ctx);

        if (diff.isEmpty())
            diff = diff(dataRows1, dataRows0, ctx);

        if (diff.isEmpty()) {
            log.info(">>>> Divergencies not detected");

            return;
        }

        testRow = diff.get(0);

        CacheDataRow r0 = dataRows0.stream().filter(r -> r.key().equals(testRow.key())).findFirst().orElse(null);
        CacheDataRow r1 = dataRows1.stream().filter(r -> r.key().equals(testRow.key())).findFirst().orElse(null);

        log.info(">>>> Test node 0 [name=" + g0.name() + ", part=" + part + ", row=" + r0 + ']');
        log.info(">>>> Test node 1 [name=" + g1.name() + ", part=" + part + ", row=" + r1 + ']');

        log.info(">>>> Test node 0 [tree=" + grpCtx0.topology().localPartition(part).dataStore().pendingTree().printTree() + ']');
        log.info(">>>> Test node 1 [tree=" + grpCtx1.topology().localPartition(part).dataStore().pendingTree().printTree() + ']');

        printKeyHistory(g0, log, testRow, grpCtx0);
        printKeyHistory(g1, log, testRow, grpCtx1);
    }

    /**
     * @param dataRows0 Data rows 0.
     * @param dataRows1 Data rows 1.
     * @param ctx Context.
     *
     * @return All rows from 0 which not the same (or not exists) in 1.
     */
    private static List<CacheDataRow> diff(
        List<CacheDataRow> dataRows0,
        List<CacheDataRow> dataRows1,
        CacheObjectValueContext ctx
    ) {
        List<CacheDataRow> diff = new ArrayList<>();

        for (CacheDataRow r0 : dataRows0) {
            boolean exists = false;

            GridCacheVersion ver0 = r0.version();
            Object k0 = r0.key().value(ctx, false);
            Object v0 = r0.value().value(ctx, false);

            for (CacheDataRow r1 : dataRows1) {
                GridCacheVersion ver1 = r1.version();
                Object k1 = r1.key().value(ctx, false);
                Object v1 = r1.value().value(ctx, false);

                if (ver0.equals(ver1) && k0.equals(k1) && (v0 != null && v0.equals(v1) || v0 == null && v1 == null )) {
                    exists = true;

                    break;
                }
            }

            if (!exists)
                diff.add(r0);
        }

        return diff;
    }

    /**
     * @param grid Grid.
     * @param log Logger.
     * @param testRow Test row.
     * @param grpCtx Group context.
     */
    public static void printKeyHistory(IgniteEx grid, IgniteLogger log, CacheDataRow testRow, CacheGroupContext grpCtx)
        throws IgniteCheckedException {
        IgniteWriteAheadLogManager walMgr = grid.context().cache().context().wal();

        if (walMgr == null)
            return;

        CacheQueryObjectValueContext ctx = new CacheQueryObjectValueContext(grpCtx.cacheObjectContext().kernalContext());

        log.info(">>>> Dumping key history [name=" + grid.name() + ", key=" + testRow.key().value(ctx, false));

        try (WALIterator iter0 = walMgr.replay(null)) {
            while (iter0.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = iter0.next();

                if (tup.get2() instanceof DataRecord) {
                    DataRecord rec = (DataRecord) tup.get2();

                    for (DataEntry entry : rec.writeEntries()) {
                        if (entry.key().equals(testRow.key()) &&
                            (entry.cacheId() == testRow.cacheId() || testRow.cacheId() == GridCacheUtils.UNDEFINED_CACHE_ID)) {
                            log.info(">>>>    " + entry +
                                ", key=" + entry.key().value(ctx, false).toString() +
                                ", val=" + (entry.value() == null ? "NULL" : entry.value().value(ctx, false).toString()));
                        }
                    }
                }
            }
        }
    }

    /**
     * Reads all rows for the partition.
     *
     * @param grpCtx Group context.
     * @param part Partition id.
     */
    public static List<CacheDataRow> rows(CacheGroupContext grpCtx, int part) throws IgniteCheckedException {
        CacheQueryObjectValueContext cacheObjCtx =
            new CacheQueryObjectValueContext(grpCtx.cacheObjectContext().kernalContext());

        List<CacheDataRow> rows = new ArrayList<>();
        grpCtx.offheap().partitionIterator(part, IgniteCacheOffheapManager.DATA_AND_TOMBSTONES).forEach(rows::add);

        for (CacheDataRow row : rows) {
            row.key().value(cacheObjCtx, false); // Unmarshal key.
            row.value().value(cacheObjCtx, false); // Unmarshal value.
        }

        return rows;
    }

    /**
     * @param consistentId Consistent id.
     */
    private static @Nullable IgniteEx grid(Object consistentId) {
        return (IgniteEx) G.allGrids().stream().filter(g -> g.cluster().localNode().consistentId().equals(consistentId)).findFirst().orElse(null);
    }
}

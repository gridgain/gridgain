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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * FreeList implementation for cache.
 */
public class CacheFreeList extends AbstractFreeList<CacheDataRow> {
    /**
     * @param cacheGrpId Cache group id.
     * @param name Name.
     * @param dataRegion Data region.
     * @param wal Wal.
     * @param metaPageId Meta page id.
     * @param initNew Initialize new.
     * @param pageFlag Default flag value for allocated pages.
     */
    public CacheFreeList(
        int cacheGrpId,
        String name,
        DataRegion dataRegion,
        @Nullable IgniteWriteAheadLogManager wal,
        long metaPageId,
        boolean initNew,
        PageLockTrackerManager pageLockTrackerManager,
        GridKernalContext ctx,
        @Nullable AtomicLong pageListCacheLimit,
        byte pageFlag
    ) throws IgniteCheckedException {
        super(
            cacheGrpId,
            name,
            dataRegion,
            null,
            wal,
            metaPageId,
            initNew,
            pageLockTrackerManager,
            ctx,
            pageListCacheLimit,
            pageFlag
        );
    }

    /** {@inheritDoc} */
    @Override public void insertDataRow(CacheDataRow row, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        super.insertDataRow(row, statHolder);

        assert row.key().partition() == PageIdUtils.partId(row.link()) :
            "Constructed a link with invalid partition ID [partId=" + row.key().partition() +
                ", link=" + U.hexLong(row.link()) + ']';
    }
}

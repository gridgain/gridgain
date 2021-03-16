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

package org.apache.ignite.internal.processors.cache.tree;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 *
 */
public class PendingRow {
    /** Expire time. */
    public long expireTime;

    /** Link. */
    public long link;

    /** Cache ID. */
    public int cacheId;

    /** */
    public Boolean tombstone;

    /** */
    public KeyCacheObject key;

    /** A cache deployment the row belongs to. */
    public IgniteUuid deploymentId;

    /**
     * Creates a new instance which represents an upper or lower bound
     * inside a logical cache.
     *
     * @param cacheId Cache ID.
     */
    public PendingRow(int cacheId) {
        this.cacheId = cacheId;
    }

    /**
     * @param cacheId Cache ID.
     * @param tombstone {@code True} if the row is created for a tombstone.
     * @param expireTime Expire time.
     * @param link Link
     */
    public PendingRow(int cacheId, boolean tombstone, long expireTime, long link) {
        this.cacheId = cacheId;
        this.tombstone = tombstone;
        this.expireTime = expireTime;
        this.link = link;
    }

    /**
     * @param grp Cache group.
     * @return Row.
     * @throws IgniteCheckedException If failed.
     */
    PendingRow initKey(CacheGroupContext grp) throws IgniteCheckedException {
        CacheDataRowAdapter rowData = grp.mvccEnabled() ? new MvccDataRow(link) : new CacheDataRowAdapter(link);
        rowData.initFromLink(grp, CacheDataRowAdapter.RowData.KEY_ONLY);

        key = rowData.key();

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PendingRow.class, this, "expireTime", expireTime, "link", link, "cacheId", cacheId,
            "tombstone", tombstone, "key", key);
    }
}

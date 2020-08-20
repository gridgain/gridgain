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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class UpdateLogRow {
    /** Cache ID. */
    @GridToStringInclude
    final int cacheId;

    /** Update counter. */
    @GridToStringInclude
    final long updCntr;

    /** Link. */
    @GridToStringInclude
    final long link;

    /** Materialized row. */
    private CacheDataRowAdapter rowData;

    /**
     * Creates a new instance which represents an upper or lower bound
     * inside a logical cache.
     *
     * @param cacheId Cache ID.
     */
    public UpdateLogRow(int cacheId) {
        this(cacheId, 0);
    }

    /**
     * @param cacheId Cache ID.
     * @param updCntr Update counter.
     * @param link Data row link.
     */
    public UpdateLogRow(int cacheId, long updCntr) {
        this(cacheId, updCntr, 0L);
    }

    /**
     * @param cacheId Cache ID.
     * @param updCntr Update counter.
     * @param link Data row link.
     */
    public UpdateLogRow(int cacheId, long updCntr, long link) {
        this.cacheId = cacheId;
        this.updCntr = updCntr;
        this.link = link;
    }

    /**
     * @return Row link.
     */
    public long link() {
        return link;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return Update counter.
     */
    public long updateCounter() {
        return updCntr;
    }

    /**
     * @param grp Cache group.
     * @return Row.
     * @throws IgniteCheckedException If failed.
     */
    UpdateLogRow initRow(CacheGroupContext grp) throws IgniteCheckedException {
        assert !grp.mvccEnabled() : "MVCC is not supported.";

        rowData = new CacheDataRowAdapter(link);

        rowData.initFromLink(grp, CacheDataRowAdapter.RowData.FULL);

        return this;
    }

    /** Updated entry key. */
    public KeyCacheObject key() {
        return rowData.key();
    }

    /** Updated entry value. */
    public CacheObject value() {
        return rowData.value();
    }

    /** Updated entry version. */
    public GridCacheVersion version() {
        return rowData.version();
    }

    /** Updated entry expire time. */
    public long expireTime() {
        return rowData.expireTime();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UpdateLogRow.class, this);
    }
}

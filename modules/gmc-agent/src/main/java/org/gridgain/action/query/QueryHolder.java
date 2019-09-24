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

package org.gridgain.action.query;

import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Query holder.
 */
public class QueryHolder implements AutoCloseable {
    /** Query ID. */
    private final String qryId;

    /** Cursors. */
    private final Map<String, CursorHolder> cursors = new HashMap<>();

    /** Cancel hook. */
    private final GridQueryCancel cancelHook = new GridQueryCancel();

    /** Accessed time. */
    private long accessedTime = System.currentTimeMillis();

    /**
     * @param qryId Query ID.
     */
    public QueryHolder(String qryId) {
        this.qryId = qryId;
    }

    /**
     * @return Query ID.
     */
    public String getQueryId() {
        return qryId;
    }

    /**
     * @return Query cursors.
     */
    public Collection<CursorHolder> getCursors() {
        return cursors.values();
    }

    /**
     * @return Query cancel hook.
     */
    public GridQueryCancel getCancelHook() {
        return cancelHook;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        cancelHook.cancel();
        cursors.keySet().forEach(this::closeCursor);
    }

    /**
     * @param cursorId Cursor ID.
     */
    public CursorHolder getCursor(String cursorId) {
        return cursors.get(cursorId);
    }

    /**
     * @param cursorId Cursor ID.
     */
    public void closeCursor(String cursorId) {
        CursorHolder cursor = getCursor(cursorId);

        if (cursor != null) {
            U.closeQuiet(cursor);
            cursors.remove(cursorId);
        }
    }

    /**
     * Update query holder accessed time.
     */
    public void updateAccessTime() {
        accessedTime = System.currentTimeMillis();
    }

    /**
     * @param ttl Ttl.
     * @return @{code true} if difference between current time and accessed time more or equals than ttl.
     */
    public boolean isExpired(long ttl) {
        return (System.currentTimeMillis() - accessedTime) >= ttl;
    }
}

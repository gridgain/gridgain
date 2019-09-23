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

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Cursor holder.
 */
public class CursorHolder implements AutoCloseable {
    /** Cursor ID. */
    private final String cursorId;

    /** Cursor. */
    private final FieldsQueryCursor<List<?>> cursor;

    /** Iterator. */
    private final Iterator<List<?>> iter;

    /**
     * @param cursorId Cursor ID.
     * @param cursor Cursor.
     * @param iter Iterator.
     */
    public CursorHolder(String cursorId, FieldsQueryCursor<List<?>> cursor, Iterator<List<?>> iter) {
        this.cursorId = cursorId;
        this.cursor = cursor;
        this.iter = iter;
    }

    /**
     * @return Cursor ID.
     */
    public String getCursorId() {
        return cursorId;
    }

    /**
     * @return Query cursor.
     */
    public FieldsQueryCursor<List<?>> getCursor() {
        return cursor;
    }

    /**
     * @return Cursor iterator.
     */
    public Iterator<List<?>> getIterator() {
        return iter;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        U.closeQuiet(cursor);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        CursorHolder that = (CursorHolder) o;

        return cursorId.equals(that.cursorId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(cursorId);
    }
}

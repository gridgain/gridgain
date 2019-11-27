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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.index.Cursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * Cursor.
 */
public class H2Cursor implements Cursor, AutoCloseable {
    /** */
    private final GridCursor<H2Row> cursor;

    /** */
    private final long time = U.currentTimeMillis();

    /** */
    private Row cur;

    /**
     * @param cursor Cursor.
     */
    public H2Cursor(GridCursor<H2Row> cursor) {
        assert cursor != null;

        this.cursor = cursor;
    }

    /** {@inheritDoc} */
    @Override public Row get() {
        return cur;
    }

    /** {@inheritDoc} */
    @Override public SearchRow getSearchRow() {
        return get();
    }

    /** {@inheritDoc} */
    @Override public boolean next() {
        try {
            while (cursor.next()) {
                H2Row row = cursor.get();

                if (row.expireTime() > 0 && row.expireTime() <= time)
                    continue;

                cur = row;

                return true;
            }

            cur = null;

            return false;
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean previous() {
        throw DbException.getUnsupportedException("previous");
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        cursor.close();
        cur = null;
    }
}

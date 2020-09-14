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
package org.gridgain.internal.h2.command.dml;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.value.ValueRow;

/**
 * In-memory group-by data for window queries.
 */
public class PlainGroupByData extends GroupByData {
    /** Rows holder. */
    private List<Object[]> rows = new ArrayList<>();

    /** Cursor for {@link #next()} method. */
    private Iterator<Object[]> cursor;

    /** Current aggregates row. */
    private Object[] curr;

    /**
     * @param ses Session.
     */
    public PlainGroupByData(Session ses) {
        super(ses);
    }

    /** {@inheritDoc} */
    @Override public Object[] nextSource(ValueRow grpKey, int width) {
        Object[] grpByData = new Object[width];

        rows.add(grpByData);

        onGroupChanged(null, null, grpByData);

        return grpByData;
    }

    /** {@inheritDoc} */
    @Override public void updateCurrent(Object[] grpByExprData) {
        Object[] old = rows.set(rows.size() - 1, grpByExprData);

        onGroupChanged(null, old, grpByExprData);
    }

    /** {@inheritDoc} */
    @Override public long size() {
        return rows.size();
    }

    /** {@inheritDoc} */
    @Override public void done(int width) {
        cursor = rows.iterator();
    }

    /** {@inheritDoc} */
    @Override public boolean next() {
        assert cursor != null;

        boolean hasNext = cursor.hasNext();

        if (hasNext)
            curr = cursor.next();
        else
            curr = null;

        return hasNext;
    }

    /** {@inheritDoc} */
    @Override public ValueRow groupKey() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Object[] groupByExprData() {
        return curr;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        rows = new ArrayList<>();
        cursor = null;
        curr = null;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        cursor.remove();
        curr = null;
    }

    @Override public void onRowProcessed() {
        // No-op.
    }
}

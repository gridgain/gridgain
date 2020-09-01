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

package org.apache.ignite.internal.processors.query.h2.disk;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.result.ResultExternal;
import org.gridgain.internal.h2.value.Value;
import org.gridgain.internal.h2.value.ValueRow;

/**
 * This class is intended for spilling to the disk (disk offloading) unsorted intermediate query results.
 */
public class PlainExternalResult extends AbstractExternalResult<Value> implements ResultExternal {
    /** In-memory buffer. */
    private List<Map.Entry<ValueRow, Value[]>> rowBuff;

    /**
     * @param ses Session.
     */
    public PlainExternalResult(Session ses) {
        super(ses, false, 0, Value.class);
    }

    /**
     * @param parent Parent result.
     */
    private PlainExternalResult(AbstractExternalResult parent) {
        super(parent);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        spillRows();

        data.rewindFile();
    }

    /** {@inheritDoc} */
    @Override public Value[] next() {
        Value[] row = data.readRowFromFile().getValue();

        return row;
    }

    /** {@inheritDoc} */
    @Override public int addRow(Value[] row) {
        addRowToBuffer(row);

        if (needToSpill())
            spillRows();

        return size;
    }

    /** {@inheritDoc} */
    @Override public int addRows(Collection<Value[]> rows) {
        if (rows.isEmpty())
            return size;

        for (Value[] row : rows)
            addRowToBuffer(row);

        if (needToSpill())
            spillRows();

        return size;
    }

    /**
     * Adds row to in-memory buffer.
     *
     * @param row Row.
     */
    private void addRowToBuffer(Value[] row) {
        if (rowBuff == null)
            rowBuff = new ArrayList<>();

        rowBuff.add(new IgniteBiTuple<>(null, row));

        long delta = H2Utils.calculateMemoryDelta(null, null, row);

        memTracker.reserve(delta);

        size++;
    }

    /**
     * Spills rows to disk.
     */
    private void spillRows() {
        if (F.isEmpty(rowBuff))
            return;

        data.store(rowBuff);

        long delta = 0;

        for (Map.Entry<ValueRow, Value[]> row : rowBuff)
            delta += H2Utils.calculateMemoryDelta(null, row.getValue(), null);

        memTracker.release(-delta);

        rowBuff.clear();
    }

    /** {@inheritDoc} */
    @Override public int removeRow(Value[] values) {
        throw new UnsupportedOperationException(); // Supported only by sorted result.
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Value[] values) {
        throw new UnsupportedOperationException(); // Supported only by sorted result.
    }

    /** {@inheritDoc} */
    @Override public synchronized ResultExternal createShallowCopy() {
        onChildCreated();

        return new PlainExternalResult(this);
    }
}

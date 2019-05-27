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

import java.util.Collection;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.result.ResultExternal;
import org.h2.store.Data;
import org.h2.value.Value;

/**
 * TODO: Add class description.
 * TODO: Add in-memory buffer with memory tracker.
 */
public class PlainExternalResult extends AbstractExternalResult {

    public PlainExternalResult(GridKernalContext ctx) {
        super(ctx);
    }

    @Override public void reset() {
        rewindFile();
    }

    @Override public Value[] next() {
        Value[] row = readRowFromFile();

        return row;
    }


    @Override public int addRow(Value[] row) {
        Data buff = createDataBuffer();

        addRowToBuffer(row, buff);

        writeBufferToFile(buff);

        return ++size;
    }

    @Override public int addRows(Collection<Value[]> rows) {
        if (rows.isEmpty())
            return size;

        Data buff = createDataBuffer();

        for (Value[] row : rows)
            addRowToBuffer(row, buff);

        writeBufferToFile(buff);

        return size += rows.size();
    }

    @Override public int removeRow(Value[] values) {
        throw new UnsupportedOperationException(); // Supported only by sorted result.
    }

    @Override public boolean contains(Value[] values) {
        throw new UnsupportedOperationException(); // Supported only by sorted result.
    }

    @Override public ResultExternal createShallowCopy() {
        // TODO: CODE: implement.

        throw new UnsupportedOperationException();
    }
}

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
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.store.Data;
import org.h2.value.Value;
import org.h2.value.ValueRow;

/**
 * TODO: Add class description.
 */
public class SortedExternalGroupByResult extends AbstractExternalResult<Object>  {
    /** Last written to file position. */
    private long lastWrittenPos;

    /** In-memory buffer for gathering rows before spilling to disk. */
    private TreeMap<ValueRow, Object[]> sortedRowsBuf;

    /** Sorted chunks addresses on the disk. */
    private final Collection<Chunk> chunks;

    /**
     * Result queue.
     */
    private Queue<Chunk> resQueue;

    /**
     * @param ses Session.
     * @param ctx Kernal context.
     * @param memTracker MemoryTracker.
     * @param initSize Initial size;
     */
    public SortedExternalGroupByResult(GridKernalContext ctx,
        Session ses,
        H2MemoryTracker memTracker,
        long initSize) {
        super(ctx, memTracker, ses.getDatabase().getCompareMode(),  "sortedGroupBy");

        chunks = new ArrayList<>();
    }


    /** {@inheritDoc} */
    public Object[] next() {
        Chunk batch = resQueue.poll();

        if (batch == null)
            throw new NoSuchElementException();

        Object[] row = batch.currentRow();

        if (batch.next())
            resQueue.offer(batch);

        return row;
    }

    /** {@inheritDoc} */
    public int addRows(Collection<Object[]> rows) {
        for (Object[] row : rows)
            addRow(row);

        return size;
    }

    /** {@inheritDoc} */
    public int addRow(Object[] row) {
        addRowToBuffer(row);

        if (needToSpill())
            spillRowsBufferToDisk();

        return size++;
    }


    /**
     * @return {@code True} if it is need to spill rows to disk.
     */
    private boolean needToSpill() {
        return !memTracker.reserved(0);
    }

    /**
     * Adds row in-memory row buffer.
     * @param row Row.
     */
    private void addRowToBuffer(Object[] row) {
        long delta = H2Utils.calculateMemoryDelta(null, null, row);

        memTracker.reserved(delta);

        if (sortedRowsBuf == null)
            sortedRowsBuf = new TreeMap<>(cmp);

        ValueRow key = getRowKey(row);

        sortedRowsBuf.put(key, row);
    }

    /**
     * Spills rows to disk from the in-memory buffer.
     */
    private void spillRowsBufferToDisk() {
        if (F.isEmpty(sortedRowsBuf))
            return;

        ArrayList<Object[]> rows =  new ArrayList<>(sortedRowsBuf.values());

        sortedRowsBuf = null;

        Data buff = createDataBuffer(rowSize(rows));

        long initFilePos = lastWrittenPos;

        for (Object[] row : rows)
            addRowToBuffer(row, buff);

        setFilePosition(initFilePos);

        long written = writeBufferToFile(buff);

        lastWrittenPos = initFilePos + written;

        chunks.add(new Chunk(initFilePos, lastWrittenPos));

        for (Object[] row : rows) {
            long delta = H2Utils.calculateMemoryDelta(null, row, null);

            memTracker.released(-delta);
        }
    }

    /** {@inheritDoc} */
    public void reset() {
        spillRowsBufferToDisk();

        if (resQueue != null) {
            resQueue.clear();

            for (Chunk chunk : chunks)
                chunk.reset();
        }
        else {
            resQueue =  new PriorityQueue<>(new Comparator<Chunk>() {
                @Override public int compare(Chunk o1, Chunk o2) {
                    int c = cmp.compare((Value)o1.currentRow()[0], (Value)o2.currentRow()[0]);

                    if (c != 0)
                        return c;

                    // Compare batches to ensure they emit rows in the arriving order.
                    return Long.compare(o1.start, o2.start);
                }
            });
        }

        // Init chunks.
        for (Chunk chunk : chunks) {
            if (chunk.next())
                resQueue.offer(chunk);
        }
    }

    /** {@inheritDoc} */
    @Override protected Object[] createEmptyArray(int colCnt) {
        return new Object[colCnt];
    }

    /** {@inheritDoc} */
    @Override protected void onClose() {
        super.onClose();
    }


    /**
     * Extracts distinct row key from the row.
     * @param row Row.
     * @return Distinct key.
     */
    public ValueRow getRowKey(Object[] row) {
        return (ValueRow)row[0];
    }

    /**
     * Sorted rows chunk on the disk.
     */
    private class Chunk {
        /** Start chunk position. */
        private final long start;

        /** End chunk position. */
        private final long end;

        /** Current position within the chunk */
        private long curPos;

        /** Current row. */
        private Object[] curRow;

        /**
         * @param start Start position.
         * @param end End position.
         */
        Chunk(long start, long end) {
            this.start = start;
            this.curPos = start;
            this.end = end;
        }

        /**
         * @return {@code True} if next row is available within a chunk.
         */
        boolean next() {
            while (curPos < end) {
                curRow = readRowFromFile(curPos);

                curPos = currentFilePosition();

                if (curRow != null)
                    return true;
            }

            return false;
        }

        /**
         * Resets position in a chunk to the begin.
         */
        void reset() {
            curPos = start;
            curRow = null;
        }

        /**
         * @return Current row.
         */
        Object[] currentRow() {
            return curRow;
        }
    }
}

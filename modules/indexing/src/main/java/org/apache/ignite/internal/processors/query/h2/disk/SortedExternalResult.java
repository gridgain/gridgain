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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.engine.Session;
import org.h2.result.ResultExternal;
import org.h2.result.SortOrder;
import org.h2.store.Data;
import org.h2.value.Value;
import org.h2.value.ValueRow;
import org.jetbrains.annotations.Nullable;

/**
 * This class is intended for spilling to the disk (disk offloading) sorted intermediate query results.
 */
@SuppressWarnings("MissortedModifiers")
public class SortedExternalResult extends AbstractExternalResult {
    /** Distinct flag. */
    private final boolean distinct;

    /** {@code DISTINCT ON(...)} expressions. */
    private final int[] distinctIndexes;

    /** Visible columns count. */
    private final int visibleColCnt;

    /** Sort order. */
    private final SortOrder sort;

    /** Last written to file position. */
    private long lastWrittenPos;

    /** In-memory buffer for gathering rows before spilling to disk. */
    private TreeMap<ValueRow, Value[]> rowsBuf;

    /** Sorted chunks addresses on disk. */
    private final Collection<Chunk> chunks = new ArrayList<>();

    /**
     * Hash index for fast lookup of the distinct rows.
     * RowKey hashcode -> list of row addressed with the same hashcode.
     */
    private ExternalResultHashIndex hashIndex;

    /**
     * Result queue.
     */
    private Queue<Chunk> resultQueue;

    /**
     * Comparator for {@code rowsBuf}.
     */
    private Comparator<Value> cmp;

    /**
     * @param ses Session.
     * @param ctx Kernal context.
     * @param distinct Distinct flag.
     * @param distinctIndexes {@code DISTINCT ON(...)} expressions.
     * @param visibleColCnt Visible columns count.
     * @param sort Sort order.
     * @param memTracker MemoryTracker.
     * @param initSize Initial size;
     */
    public SortedExternalResult(GridKernalContext ctx,
        Session ses,
        boolean distinct,
        int[] distinctIndexes,
        int visibleColCnt,
        SortOrder sort,
        H2MemoryTracker memTracker,
        long initSize) {
        super(ctx, memTracker);

        this.distinct = distinct;
        this.distinctIndexes = distinctIndexes;
        this.visibleColCnt = visibleColCnt;
        this.sort = sort;
        this.cmp = ses.getDatabase().getCompareMode();

        if (isAnyDistinct())
            hashIndex = new ExternalResultHashIndex(ctx, file, this, initSize);
    }

    /** {@inheritDoc} */
    @Override public Value[] next() {
        Chunk batch = resultQueue.poll();

        if (batch == null)
            throw new NoSuchElementException();

        Value[] row = batch.currentRow();

        if (batch.next())
            resultQueue.offer(batch);

        return row;
    }

    /** {@inheritDoc} */
    @Override public int addRows(Collection<Value[]> rows) {
        for (Value[] row : rows)
            addRow(row);

        return size;
    }

    /** {@inheritDoc} */
    @Override public int addRow(Value[] values) {
        if (isAnyDistinct()) {
            if (containsRowWithOrderCheck(values))
                return size;
        }

        addRowToBuffer(values);

        if (needToSpill())
            spillRowsBufferToDisk();

        return size++;
    }

    /**
     * Checks if current result contains given row with sort order check.
     *
     * @param row Row.
     * @return {@code True} if current result does not contain th given row.
     */
    private boolean containsRowWithOrderCheck(Value[] row) { // TODO merge removeRow and getPreviousRow
        Value[] previous = getPreviousRow(row);

        if (previous == null)
            return false;

        if (sort != null && sort.compare(previous, row) > 0) {
            removeRow(row); // It is need to replace old row with a new one because of sort order.

            return false;
        }

        return true;
    }

    /**
     * Returns the previous row.
     *
     * @param row Row.
     * @return Previous row.
     */
    @Nullable private Value[] getPreviousRow(Value[] row) { // TODO merge removeRow and getPreviousRow
        ValueRow distKey = getRowKey(row);

        Value[] previous = null;

        // Check in memory - it might not has been spilled yet.
        if (rowsBuf != null) {
            previous = rowsBuf.get(distKey);

            if (previous != null)
                return previous;
        }

        // Check on-disk
        Long addr = hashIndex.get(distKey);

        if (addr == null)
            return null;

        previous = readRowFromFile(addr);

        return previous;
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
    private void addRowToBuffer(Value[] row) {
        if (rowsBuf == null)
            rowsBuf = new TreeMap<>(cmp);

        ValueRow key = getRowKey(row);

        long delta = H2Utils.calculateMemoryDelta(null, null, row);

        memTracker.reserved(delta);

        rowsBuf.put(key, row);
    }

    /**
     * Spills rows to disk from the in-memory buffer.
     */
    private void spillRowsBufferToDisk() {
        if (F.isEmpty(rowsBuf))
            return;

        ArrayList<Value[]> rows = new ArrayList<>(rowsBuf.values());

        for (Map.Entry<ValueRow, Value[]> e : rowsBuf.entrySet()) {
            long delta = H2Utils.calculateMemoryDelta(null, e.getValue(), null);

            memTracker.released(-delta);
        }

        rowsBuf = null;

        if (sort != null)
            sort.sort(rows);

        Data buff = createDataBuffer();

        long initFilePos = lastWrittenPos;


        for (Value[] row : rows) {
            if(isAnyDistinct()) {
                long rowPosInBuff = buff.length();

                addRowToHashIndex(row, initFilePos + rowPosInBuff);
            }

            addRowToBuffer(row, buff);
        }

        setFilePosition(initFilePos);

        long written = writeBufferToFile(buff);

        lastWrittenPos = initFilePos + written;

        chunks.add(new Chunk(initFilePos, lastWrittenPos));
    }

    /**
     * Adds row to hash index.
     *
     * @param row Row.
     * @param rowPosInFile Row position in file.
     */
    private void addRowToHashIndex(Value[] row, long rowPosInFile) {
        ValueRow distKey = getRowKey(row);

        hashIndex.put(distKey, rowPosInFile);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        spillRowsBufferToDisk();

        if (resultQueue != null) {
            resultQueue.clear();

            for (Chunk chunk : chunks)
                chunk.reset();
        }
        else {
            resultQueue = sort == null ? new LinkedList<>() : new PriorityQueue<>(new Comparator<Chunk>() {
                @Override public int compare(Chunk o1, Chunk o2) {
                    return sort.compare(o1.currentRow(), o2.currentRow());
                }
            });
        }

        // Init chunks.
        for (Chunk chunk : chunks) {
            if (chunk.next())
                resultQueue.offer(chunk);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(fileIo);
        U.closeQuiet(hashIndex);
    }

    /** {@inheritDoc} */
    @Override public int removeRow(Value[] values) { // TODO merge removeRow and getPreviousRow
        ValueRow key = getRowKey(values);

        if (rowsBuf != null) {
            Object prev = rowsBuf.remove(key);

            if (prev != null)
                return size--;
        }

        // Check on-disk
        Long addr = hashIndex.remove(key);

        if (addr == null)
            return size;

        Value[] res = readRowFromFile(addr);

        if (res == null)
            return size;

        markRowRemoved(addr);

        return size--;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Value[] values) {
        return getPreviousRow(values) != null;
    }

    /** {@inheritDoc} */
    @Override public ResultExternal createShallowCopy() {
        //return null; // TODO: CODE: implement.

        throw new UnsupportedOperationException();
    }

    /**
     * @return whether this result is a distinct result
     */
    private boolean isAnyDistinct() {
        return distinct || distinctIndexes != null;
    }

    /**
     * Extracts distinct row key from the row.
     * @param row Row.
     * @return Distinct key.
     */
    public ValueRow getRowKey(Value[] row) {
        if (distinctIndexes != null) {
            int cnt = distinctIndexes.length;

            Value[] newValues = new Value[cnt];

            for (int i = 0; i < cnt; i++)
                newValues[i] = row[distinctIndexes[i]];

            row = newValues;
        } else if (row.length > visibleColCnt)
            row = Arrays.copyOf(row, visibleColCnt);

        return ValueRow.get(row);
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
        private Value[] curRow;

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
                curRow = readRowFromFile(curPos); // TODO read multiple rows and cache it if possible.

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
        Value[] currentRow() {
            return curRow;
        }
    }
}

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
    private TreeMap<ValueRow, Value[]> sortedRowsBuf;

    /** In-memory buffer for gathering rows before spilling to disk. */
    private ArrayList<Value[]> unsortedRowsBuf;

    /** Sorted chunks addresses on the disk. */
    private final Collection<Chunk> chunks;

    /**
     * Hash index for fast lookup of the distinct rows.
     * RowKey -> Row address in the row store.
     */
    private ExternalResultHashIndex hashIdx;

    /**
     * Result queue.
     */
    private Queue<Chunk> resQueue;

    /**
     * Comparator for {@code sortedRowsBuf}.
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
        cmp = ses.getDatabase().getCompareMode();
        chunks = new ArrayList<>();

        if (isAnyDistinct())
            hashIdx = new ExternalResultHashIndex(ctx, file, this, initSize);
    }

    /**
     * @param parent Parent.
     */
    private SortedExternalResult(SortedExternalResult parent) {
        super(parent);

        distinct = parent.distinct;
        distinctIndexes = parent.distinctIndexes;
        visibleColCnt = parent.visibleColCnt;
        sort = parent.sort;
        cmp = parent.cmp;
        hashIdx = parent.hashIdx;
        chunks = parent.chunks;
    }

    /** {@inheritDoc} */
    @Override public Value[] next() {
        Chunk batch = resQueue.poll();

        if (batch == null)
            throw new NoSuchElementException();

        Value[] row = batch.currentRow();

        if (batch.next())
            resQueue.offer(batch);

        return row;
    }

    /** {@inheritDoc} */
    @Override public int addRows(Collection<Value[]> rows) {
        for (Value[] row : rows)
            addRow(row);

        return size;
    }

    /** {@inheritDoc} */
    @Override public int addRow(Value[] row) {
        if (isAnyDistinct() && containsRowWithOrderCheck(row))
                return size;

        addRowToBuffer(row);

        if (needToSpill())
            spillRowsBufferToDisk();

        return ++size;
    }

    /**
     * Checks if current result contains given row with sort order check.
     *
     * @param row Row.
     * @return {@code True} if current result does not contain the given row.
     */
    private boolean containsRowWithOrderCheck(Value[] row) {
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
    @Nullable private Value[] getPreviousRow(Value[] row) {
        assert unsortedRowsBuf == null;

        ValueRow distKey = getRowKey(row);

        Value[] previous;

        // Check in memory - it might not has been spilled yet.
        if (sortedRowsBuf != null) {
            previous = sortedRowsBuf.get(distKey);

            if (previous != null)
                return previous;
        }

        // Check on-disk
        Long addr = hashIdx.get(distKey);

        if (addr == null)
            return null;

        previous = readRowFromFile(addr);

        return previous;
    }

    /** {@inheritDoc} */
    @Override public int removeRow(Value[] values) {
        ValueRow key = getRowKey(values);

        if (sortedRowsBuf != null) {
            Object prev = sortedRowsBuf.remove(key);

            if (prev != null)
                return size--;
        }

        // Check on-disk
        Long addr = hashIdx.remove(key);

        if (addr == null)
            return size;

        Value[] res = readRowFromFile(addr);

        if (res == null)
            return size;

        markRowRemoved(addr);

        return --size;
    }

    /**
     * @return {@code True} if it is need to spill rows to disk.
     */
    private boolean needToSpill() {
        return !memTracker.reserved(0);
    }

    /**
     * Adds row to in-memory row buffer.
     * @param row Row.
     */
    private void addRowToBuffer(Value[] row) {
        long delta = H2Utils.calculateMemoryDelta(null, null, row);

        memTracker.reserved(delta);

        if (isAnyDistinct()) {
            assert unsortedRowsBuf == null;

            if (sortedRowsBuf == null)
                sortedRowsBuf = new TreeMap<>(cmp);

            ValueRow key = getRowKey(row);

            sortedRowsBuf.put(key, row);
        }
        else {
            assert sortedRowsBuf == null;

            if (unsortedRowsBuf == null)
                unsortedRowsBuf = new ArrayList<>();

            unsortedRowsBuf.add(row);
        }
    }

    /**
     * Spills rows to disk from the in-memory buffer.
     */
    private void spillRowsBufferToDisk() {
        if (F.isEmpty(sortedRowsBuf) && F.isEmpty(unsortedRowsBuf))
            return;

        ArrayList<Value[]> rows = isAnyDistinct() ? new ArrayList<>(sortedRowsBuf.values()) : unsortedRowsBuf;

        sortedRowsBuf = null;
        unsortedRowsBuf = null;

        if (sort != null)
            sort.sort(rows);

        Data buff = createDataBuffer(rowSize(rows));

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

        for (Value[] row : rows) {
            long delta = H2Utils.calculateMemoryDelta(null, row, null);

            memTracker.released(-delta);
        }
    }

    /**
     * Adds row to hash index.
     *
     * @param row Row.
     * @param rowPosInFile Row position in file.
     */
    private void addRowToHashIndex(Value[] row, long rowPosInFile) {
        ValueRow distKey = getRowKey(row);

        hashIdx.put(distKey, rowPosInFile);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        spillRowsBufferToDisk();

        if (resQueue != null) {
            resQueue.clear();

            for (Chunk chunk : chunks)
                chunk.reset();
        }
        else {
            resQueue = sort == null ? new LinkedList<>() : new PriorityQueue<>(new Comparator<Chunk>() {
                @Override public int compare(Chunk o1, Chunk o2) {
                    return sort.compare(o1.currentRow(), o2.currentRow());
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
    @Override protected void onClose() {
        super.onClose();

        U.closeQuiet(hashIdx);
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Value[] values) {
        return getPreviousRow(values) != null;
    }

    /** {@inheritDoc} */
    @Override public synchronized ResultExternal createShallowCopy() {
        onChildCreated();

        return new SortedExternalResult(this);
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
    ValueRow getRowKey(Value[] row) {
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
        Value[] currentRow() {
            return curRow;
        }
    }
}

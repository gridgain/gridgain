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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.result.ResultExternal;
import org.gridgain.internal.h2.result.SortOrder;
import org.gridgain.internal.h2.value.Value;
import org.gridgain.internal.h2.value.ValueRow;

/**
 * This class is intended for spilling to the disk (disk offloading) sorted intermediate query results.
 */
public class SortedExternalResult extends AbstractExternalResult<Value> implements ResultExternal {
    /** Distinct flag. */
    private final boolean distinct;

    /** {@code DISTINCT ON(...)} expressions. */
    private final int[] distinctIndexes;

    /** Visible columns count. */
    private final int visibleColCnt;

    /** Sort order. */
    private final SortOrder sort;

    /** In-memory buffer for gathering rows before spilling to disk. */
    private TreeMap<ValueRow, Value[]> sortedRowsBuf;

    /** In-memory buffer for gathering rows before spilling to disk. */
    private ArrayList<Value[]> unsortedRowsBuf;

    /** Result queue. */
    private Queue<ExternalResultData.Chunk> resQueue;

    /**  Comparator for {@code sortedRowsBuf}. It is used to prevent duplicated rows keys in {@code sortedRowsBuf}. */
    private Comparator<Value> cmp;

    /** Chunks comparator. */
    private final Comparator<ExternalResultData.Chunk> chunkCmp;

    /**
     * @param ses Session.
     * @param distinct Distinct flag.
     * @param distinctIndexes {@code DISTINCT ON(...)} expressions.
     * @param visibleColCnt Visible columns count.
     * @param sort Sort order.
     * @param initSize Initial size;
     */
    public SortedExternalResult(Session ses,
        boolean distinct,
        int[] distinctIndexes,
        int visibleColCnt,
        SortOrder sort,
        long initSize) {
        super(ses, isDistinct(distinct, distinctIndexes), initSize, Value.class);

        this.distinct = isDistinct(distinct, distinctIndexes);
        this.distinctIndexes = distinctIndexes;
        this.visibleColCnt = visibleColCnt;
        this.sort = sort;
        this.cmp = ses.getDatabase().getCompareMode();
        this.chunkCmp = new Comparator<ExternalResultData.Chunk>() {
            @Override public int compare(ExternalResultData.Chunk o1, ExternalResultData.Chunk o2) {
                int c = sort.compare((Value[])o1.currentRow().getValue(),(Value[]) o2.currentRow().getValue());

                if (c != 0)
                    return c;

                // Compare batches to ensure they emit rows in the arriving order.
                return Long.compare(o1.start(), o2.start());
            }
        };
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
        chunkCmp = parent.chunkCmp;
    }

    /** {@inheritDoc} */
    @Override public Value[] next() {
        ExternalResultData.Chunk batch = resQueue.poll();

        if (batch == null)
            throw new NoSuchElementException();

        Value[] row = (Value[])batch.currentRow().getValue();

        if (batch.next())
            resQueue.offer(batch);

        return row;
    }

    /** {@inheritDoc} */
    @Override public int addRows(Collection<Value[]> rows) {
        for (Value[] row : rows) {
            if (distinct && containsRowWithOrderCheck(row))
                continue;

            addRowToBuffer(row);

            size++;
        }

        if (needToSpill())
            spillRowsBufferToDisk();

        return size;
    }

    /** {@inheritDoc} */
    @Override public int addRow(Value[] row) {
        if (distinct && containsRowWithOrderCheck(row))
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
     * @return {@code True} if current result contains the given row.
     */
    private boolean containsRowWithOrderCheck(Value[] row) {
        assert unsortedRowsBuf == null;

        ValueRow distKey = getRowKey(row);

        Value[] previous = sortedRowsBuf == null ? null : sortedRowsBuf.get(distKey);

        if (previous != null) {
            if (sort != null && sort.compare(previous, row) > 0) {
                sortedRowsBuf.remove(distKey); // It is need to replace old row with a new one because of sort order.

                size--;

                return false;
            }

            return true;
        }

        Map.Entry<ValueRow, Value[]> prevRow = data.get(distKey);

        previous = prevRow == null ? null : prevRow.getValue();

        if (previous == null)
            return false;

        if (sort != null && sort.compare(previous, row) > 0) {
            data.remove(distKey); // It is need to replace old row with a new one because of sort order.

            size--;

            return false;
        }

        return true;
    }

    /**
     * @param distinct Distinct flag.
     * @param distinctIndexes Distinct indexes.
     * @return {@code True} if this is a distinct result.
     */
    private static boolean isDistinct(boolean distinct, int[] distinctIndexes) {
        return distinct || distinctIndexes != null;
    }


    /** {@inheritDoc} */
    @Override public boolean contains(Value[] values) {
        ValueRow key = getRowKey(values);

        if (!F.isEmpty(sortedRowsBuf)) {
            if (sortedRowsBuf.containsKey(key))
                return true;
        }

        return data.contains(key);
    }

    /** {@inheritDoc} */
    @Override public int removeRow(Value[] values) {
        ValueRow key = getRowKey(values);

        if (sortedRowsBuf != null) {
            Object prev = sortedRowsBuf.remove(key);

            if (prev != null)
                return --size;
        }

        // Check on-disk
        if (data.remove(key))
            --size;

        return size;
    }

    /**
     * Adds row to in-memory row buffer.
     * @param row Row.
     */
    private void addRowToBuffer(Value[] row) {
        if (distinct) {
            assert unsortedRowsBuf == null;

            if (sortedRowsBuf == null)
                sortedRowsBuf = new TreeMap<>(cmp);

            ValueRow key = getRowKey(row);

            Value[] old = sortedRowsBuf.put(key, row);

            long delta = H2Utils.calculateMemoryDelta(key, old, row);

            memTracker.reserve(delta);
        }
        else {
            assert sortedRowsBuf == null;

            if (unsortedRowsBuf == null)
                unsortedRowsBuf = new ArrayList<>();

            unsortedRowsBuf.add(row);

            long delta = H2Utils.calculateMemoryDelta(null, null, row);

            memTracker.reserve(delta);
        }
    }

    /**
     * Spills rows to disk from the in-memory buffer.
     */
    private void spillRowsBufferToDisk() {
        if (F.isEmpty(sortedRowsBuf) && F.isEmpty(unsortedRowsBuf))
            return;

        int size = distinct ? sortedRowsBuf.size() : unsortedRowsBuf.size();

        List<Map.Entry<ValueRow, Value[]>> rows = new ArrayList<>(size);

        if (distinct) {
            for (Map.Entry<ValueRow, Value[]> e : sortedRowsBuf.entrySet())
                rows.add(new IgniteBiTuple<>(e.getKey(), e.getValue()));
        }
        else {
            for (Value[] row : unsortedRowsBuf)
                rows.add(new IgniteBiTuple<>(null, row));
        }

        sortedRowsBuf = null;
        unsortedRowsBuf = null;

        if (sort != null)
            rows.sort((o1, o2) -> sort.compare(o1.getValue(), o2.getValue()));

        data.store(rows);

        long delta = 0;

        for (Map.Entry<ValueRow, Value[]> row : rows)
            delta += H2Utils.calculateMemoryDelta(row.getKey(), row.getValue(), null);

        memTracker.release(-delta);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        spillRowsBufferToDisk();

        if (resQueue != null) {
            resQueue.clear();

            for (ExternalResultData.Chunk chunk : data.chunks())
                chunk.reset();
        }
        else {
            resQueue = sort == null ? new ArrayDeque<>() : new PriorityQueue<>(chunkCmp);
        }

        // Init chunks.
        for (ExternalResultData.Chunk chunk : data.chunks()) {
            if (chunk.next())
                resQueue.offer(chunk);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized ResultExternal createShallowCopy() {
        onChildCreated();

        return new SortedExternalResult(this);
    }

    /**
     * Extracts distinct row key from the row.
     * @param row Row.
     * @return Distinct key.
     */
    private ValueRow getRowKey(Value[] row) {
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
}

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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.util.GridLongList;
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
 * TODO: Add class description.
 */
@SuppressWarnings("MissortedModifiers")
public class SortedExternalResult extends AbstractExternalResult {

    private final boolean distinct;
    private final int[] distinctIndexes;
    private final int visibleColCnt;
    private final SortOrder sort;
    private final H2MemoryTracker memTracker;
    private long lastWrittenPos;

    private TreeMap<ValueRow, Value[]> rowsBuffer;

    private final Collection<Chunk> chunks = new ArrayList<>();

    // TODO replace with a disk-based map
    private HashMap<Integer, GridLongList> hashIndex;

    private Queue<Chunk> resultQueue;

    private Comparator cmp;

    /**
     *
     * @param ctx Kernal context.
     * @param distinct Distinct flag.
     * @param distinctIndexes {@code DISTINCT ON(...)} expressions.
     * @param visibleColCnt Visible columns count.
     * @param sort Sort order.
     * @param memTracker MemoryTracker.
     */
    public SortedExternalResult(GridKernalContext ctx,
        Session ses,
        boolean distinct,
        int[] distinctIndexes,
        int visibleColCnt,
        SortOrder sort,
        H2MemoryTracker memTracker) {
        super(ctx);

        this.distinct = distinct;
        this.distinctIndexes = distinctIndexes;
        this.visibleColCnt = visibleColCnt;
        this.sort = sort;
        this.memTracker = memTracker;
        this.cmp = ses.getDatabase().getCompareMode();

        if (isAnyDistinct())
            hashIndex = new HashMap<>();
    }

    @Override public Value[] next() {
        Chunk batch = resultQueue.poll();

        if (batch == null)
            throw new NoSuchElementException();

        Value[] row = batch.currentRow();

        if (batch.next())
            resultQueue.offer(batch);

        return row;
    }

    @Override public int addRows(Collection<Value[]> rows) {
        for (Value[] row : rows)
            addRow(row);

        return size;
    }

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

     // TODO merge removeRow and getPreviousRow
    private boolean containsRowWithOrderCheck(Value[] values) {
        Value[] previous = getPreviousRow(values);

        if (previous == null)
            return false;

        if (sort != null && sort.compare(previous, values) > 0) {
            removeRow(values);

            return false;
        }

        return true;
    }

    // TODO merge removeRow and getPreviousRow
    @Nullable private Value[] getPreviousRow(Value[] values) {
        ValueRow distKey = getRowKey(values);

        Value[] previous = null;

        // Check in memory - it might not has been spilled yet.
        if (rowsBuffer != null) {
            previous = rowsBuffer.get(distKey);

            if (previous != null)
                return previous;
        }

        // Check on-disk
        GridLongList addrs = hashIndex.get(distKey.hashCode());

        if (addrs != null) {
            for (int i = 0; i < addrs.size(); i++) {
                setFilePosition(addrs.get(i));

                Value[] res = readRowFromFile();

                if (res == null)
                    continue;

                if (distKey.equals(getRowKey(res))) {
                    previous = res;

                    break;
                }
            }
        }

        return previous;
    }

    private boolean needToSpill() {
        return !memTracker.allocate(0);
    }

    private void addRowToBuffer(Value[] row) {
        if (rowsBuffer == null)
            rowsBuffer = new TreeMap<>(cmp);

        ValueRow key = getRowKey(row);

        long delta = H2Utils.calculateMemoryDelta(null, null, row);

        memTracker.allocate(delta);

        rowsBuffer.put(key, row);
    }

    private void spillRowsBufferToDisk() {
        if (F.isEmpty(rowsBuffer))
            return;

        ArrayList<Value[]> rows = new ArrayList<>(rowsBuffer.values());

        for (Map.Entry<ValueRow, Value[]> e : rowsBuffer.entrySet()) {
            long delta = H2Utils.calculateMemoryDelta(null, e.getValue(), null);

            memTracker.free(-delta);
        }

        rowsBuffer = null;

        if (sort != null)
            sort.sort(rows);

        Data buff = createDataBuffer();

        long initFilePos = lastWrittenPos;

        setFilePosition(initFilePos);

        for (Value[] row : rows) {
            if(isAnyDistinct()) {
                long rowPosInBuff = buff.length();

                addRowToHashIndex(row, initFilePos + rowPosInBuff);
            }

            addRowToBuffer(row, buff);
        }

        long written = writeBufferToFile(buff);

        lastWrittenPos = initFilePos + written;

        chunks.add(new Chunk(initFilePos, lastWrittenPos));
    }

    private void addRowToHashIndex(Value[] row, long rowPosInFile) {
        ValueRow distKey = getRowKey(row);

        GridLongList addrs = hashIndex.computeIfAbsent(distKey.hashCode(), k -> new GridLongList());

        addrs.add(rowPosInFile);
    }

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

    @Override public void close() {
        U.closeQuiet(fileChannel);
    }

    // TODO merge removeRow and getPreviousRow
    @Override public int removeRow(Value[] values) {
        ValueRow key = getRowKey(values);

        if (rowsBuffer != null) {
            Object prev = rowsBuffer.remove(key);

            if (prev != null)
                return size--;
        }

        // Check on-disk
        GridLongList addrs = hashIndex.get(key.hashCode());

        if (addrs != null) {
            for (int i = 0; i < addrs.size(); i++) {
                long addr = addrs.get(i);

                setFilePosition(addr);

                Value[] res = readRowFromFile();

                if (res == null)
                    continue;

                if (key.equals(getRowKey(res))) {
                    markRowRemoved(addr);

                    return size--;
                }
            }
        }

        return size; // Nothing was removed.
    }


    @Override public boolean contains(Value[] values) {
        return getPreviousRow(values) != null;
    }

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

    private ValueRow getRowKey(Value[] values) {
        if (distinctIndexes != null) {
            int cnt = distinctIndexes.length;

            Value[] newValues = new Value[cnt];

            for (int i = 0; i < cnt; i++)
                newValues[i] = values[distinctIndexes[i]];

            values = newValues;
        } else if (values.length > visibleColCnt)
            values = Arrays.copyOf(values, visibleColCnt);

        return ValueRow.get(values);
    }

    private class Chunk {
        private final long start;
        private final long end;

        private long curPos;

        private Value[] curRow;

        Chunk(long start, long end) {
            this.start = start;
            this.curPos = start;
            this.end = end;
        }

        boolean next() {
            while (curPos < end) {
                setFilePosition(curPos);

                curRow = readRowFromFile();

                curPos = currentFilePosition();

                if (curRow != null)
                    return true;
            }

            return false;
        }

        void reset() {
            curPos = start;
            curRow = null;
        }

        Value[] currentRow() {
            return curRow;
        }
    }
}

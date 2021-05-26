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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.ignite.IgniteSystemProperties;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.engine.SessionInterface;
import org.gridgain.internal.h2.expression.Expression;
import org.gridgain.internal.h2.message.DbException;
import org.gridgain.internal.h2.result.LocalResult;
import org.gridgain.internal.h2.result.ResultExternal;
import org.gridgain.internal.h2.result.SortOrder;
import org.gridgain.internal.h2.util.Utils;
import org.gridgain.internal.h2.value.TypeInfo;
import org.gridgain.internal.h2.value.Value;
import org.gridgain.internal.h2.value.ValueRow;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_DISTINCT_RESULTS_USE_TREE_MAP;
import static org.apache.ignite.internal.processors.query.h2.H2Utils.calculateMemoryDelta;

/** */
public class H2ManagedLocalResult implements LocalResult {
    /** Use  */
    private static boolean USE_TREEMAP = IgniteSystemProperties.getBoolean(IGNITE_SQL_DISTINCT_RESULTS_USE_TREE_MAP, false);

    /** */
    private Session session;

    /** */
    private int visibleColumnCount;

    /** */
    private Expression[] expressions;

    /** */
    private int rowId;

    /** */
    private int rowCount;

    /** */
    private ArrayList<Value[]> rows;

    /** */
    private SortOrder sort;

    /** */
    private Map<Value, Value[]> distinctRows;

    /** */
    private Value[] currentRow;

    /** */
    private int offset;

    /** */
    private int limit = -1;

    /** */
    private boolean fetchPercent;

    /** */
    private SortOrder withTiesSortOrder;

    /** */
    private boolean limitsWereApplied;

    /** */
    private boolean distinct;

    /** */
    private int[] distinctIndexes;

    /** */
    private boolean closed;

    /** */
    private boolean containsLobs;

    /** */
    private Boolean containsNull;

    /** Disk spilling (offloading) manager. */
    private ResultExternal external;

    /** Query memory tracker. */
    private H2MemoryTracker memTracker;

    /** Reserved memory. */
    private long memReserved;

    /**
     * Construct a local result object.
     */
    public H2ManagedLocalResult() {
        // nothing to do
    }

    /**
     * Constructor.
     *
     * @param ses the session
     * @param expressions the expression array
     * @param visibleColCnt the number of visible columns
     */
    public H2ManagedLocalResult(Session ses, Expression[] expressions,
        int visibleColCnt) {
        this.session = ses;
        rows = Utils.newSmallArrayList();
        this.visibleColumnCount = visibleColCnt;
        rowId = -1;
        this.expressions = expressions;

        memTracker = session.memoryTracker();
    }

    /**
     * Checks available memory.
     *
     * @param distinctRowKey Row key.
     * @param oldRow Old row.
     * @param row New row.
     * @return {@code True} if we have available memory.
     */
    private boolean hasAvailableMemory(ValueRow distinctRowKey, Value[] oldRow, Value[] row) {
        assert !isClosed();

        if (memTracker == null)
            return true; // No memory management set.

        long memory = calculateMemoryDelta(distinctRowKey, oldRow, row);

        boolean hasMemory = true;

        if (memory < 0)
            memTracker.release(-memory);
        else
            hasMemory = memTracker.reserve(memory);

        memReserved += memory;

        return hasMemory;
    }

    /** {@inheritDoc} */
    @Override public boolean isLazy() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void setMaxMemoryRows(int maxValue) {
        // No-op. We do not use rowCount-based memory tracking in this class. {@link memTracker} is used instead.
    }

    /** {@inheritDoc} */
    @Override public H2ManagedLocalResult createShallowCopy(SessionInterface targetSession) {
        if (containsLobs) {
            return null;
        }

        ResultExternal e2 = null;

        if (external != null) {
            e2 = external.createShallowCopy();

            if (e2 == null)
                return null;
        }

        H2ManagedLocalResult cp = new H2ManagedLocalResult();

        cp.session = (Session)targetSession;
        cp.visibleColumnCount = this.visibleColumnCount;
        cp.expressions = this.expressions;
        cp.rowId = -1;
        cp.rowCount = this.rowCount;
        cp.rows = this.rows;
        cp.sort = this.sort;
        cp.distinctRows = this.distinctRows;
        cp.distinct = distinct;
        cp.distinctIndexes = distinctIndexes;
        cp.currentRow = null;
        cp.offset = 0;
        cp.limit = -1;
        cp.containsNull = containsNull;
        cp.external = e2;

        return cp;
    }

    /** {@inheritDoc} */
    @Override public void setSortOrder(SortOrder sort) {
        this.sort = sort;
    }

    /** {@inheritDoc} */
    @Override public void setDistinct() {
        assert distinctIndexes == null;
        distinct = true;
        distinctRows = createDistinctMap();
    }

    /** {@inheritDoc} */
    @Override public void setDistinct(int[] distinctIndexes) {
        assert !distinct;
        this.distinctIndexes = distinctIndexes;
        distinctRows = createDistinctMap();
    }

    /**
     * @return whether this result is a distinct result
     */
    private boolean isAnyDistinct() {
        return distinct || distinctIndexes != null;
    }

    /** {@inheritDoc} */
    @Override public void removeDistinct(Value[] values) {
        if (!distinct) {
            DbException.throwInternalError();
        }
        assert values.length == visibleColumnCount;
        if (distinctRows != null) {
            ValueRow array = ValueRow.get(values);
            distinctRows.remove(array);
            rowCount = distinctRows.size();
        }
        // Add new row.
        else {
            rowCount = external.removeRow(values);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsDistinct(Value[] values) {
        assert values.length == visibleColumnCount;
        if (external != null) {
            return external.contains(values);
        }
        if (distinctRows == null) {
            distinctRows = createDistinctMap();
            for (Value[] row : rows) {
                ValueRow array = getDistinctRow(row);
                distinctRows.put(array, array.getList());
            }
        }
        ValueRow array = ValueRow.get(values);
        return distinctRows.get(array) != null;
    }

    /** {@inheritDoc} */
    @Override public boolean containsNull() {
        Boolean r = containsNull;
        if (r == null) {
            r = false;
            reset();
            loop:
            while (next()) {
                Value[] row = currentRow;
                for (int i = 0; i < visibleColumnCount; i++) {
                    if (row[i].containsNull()) {
                        r = true;
                        break loop;
                    }
                }
            }
            reset();
            containsNull = r;
        }
        return r;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        rowId = -1;
        currentRow = null;
        if (external != null)
            external.reset();
    }

    /** {@inheritDoc} */
    @Override public Value[] currentRow() {
        return currentRow;
    }

    /** {@inheritDoc} */
    @Override public boolean next() {
        if (!closed && rowId < rowCount) {
            rowId++;
            if (rowId < rowCount) {
                if (external != null)
                    currentRow = external.next();
                else
                    currentRow = rows.get(rowId);

                return true;
            }
            currentRow = null;
        }
        return false;
    }

    /** {@inheritDoc} */
    @Override public int getRowId() {
        return rowId;
    }

    /** {@inheritDoc} */
    @Override public boolean isAfterLast() {
        return rowId >= rowCount;
    }

    /**
     * @param values Values.
     */
    private void cloneLobs(Value[] values) {
        for (int i = 0; i < values.length; i++) {
            Value v = values[i];
            Value v2 = v.copyToResult();
            if (v2 != v) {
                containsLobs = true;
                session.addTemporaryLob(v2);
                values[i] = v2;
            }
        }
    }

    /**
     * @param values row.
     * @return Row,
     */
    private ValueRow getDistinctRow(Value[] values) {
        if (distinctIndexes != null) {
            int cnt = distinctIndexes.length;
            Value[] newValues = new Value[cnt];
            for (int i = 0; i < cnt; i++) {
                newValues[i] = values[distinctIndexes[i]];
            }
            values = newValues;
        }
        else if (values.length > visibleColumnCount) {
            values = Arrays.copyOf(values, visibleColumnCount);
        }
        return ValueRow.get(values);
    }

    private void createExternalResult(boolean forcePlainResult) {
        QueryMemoryManager memMgr = (QueryMemoryManager)session.groupByDataFactory();
        if (forcePlainResult)
            external = memMgr.createPlainExternalResult(session);
        else {
            external = distinct || distinctIndexes != null || sort != null ?
                memMgr.createSortedExternalResult(session, distinct, distinctIndexes, visibleColumnCount, sort, rowCount)
                : memMgr.createPlainExternalResult(session);
        }
    }

    /** {@inheritDoc} */
    @Override public void addRow(Value[] values) {
        cloneLobs(values);
        if (isAnyDistinct()) {
            if (distinctRows != null) {
                ValueRow array = getDistinctRow(values);
                Value[] previous = distinctRows.get(array);
                if (previous == null || sort != null && sort.compare(previous, values) > 0) {
                    distinctRows.put(array, values);
                }
                rowCount = distinctRows.size();
                if (!hasAvailableMemory(array, previous, values)) {
                    addRowsToDisk(false);

                    distinctRows = null;
                }
            } else {
                rowCount = external.addRow(values);
            }
        } else {
            rowCount++;
            if (external == null) {
                rows.add(values);
                if (!hasAvailableMemory(null, null, values)) {
                    addRowsToDisk(false);
                }
            }
            else
                external.addRow(values);
        }
    }

    /**
     * Adds rows to disk.
     * @param forcePlainResult Whether to force creation of not sorted result.
     */
    private void addRowsToDisk(boolean forcePlainResult) {
        if (external == null) {
            createExternalResult(forcePlainResult);
        }

        if (distinctRows == null) {
            rowCount = external.addRows(rows);
            rows.clear();
        }
        else {
            rowCount = external.addRows(distinctRows.values());
            distinctRows.clear();
        }

        memTracker.release(memReserved);

        memReserved = 0;
    }

    /** {@inheritDoc} */
    @Override public int getVisibleColumnCount() {
        return visibleColumnCount;
    }

    /** {@inheritDoc} */
    @Override public void done() {
        if (external != null)
            addRowsToDisk(false);

        else {
            if (isAnyDistinct())
                rows = new ArrayList<>(distinctRows.values());

            if (sort != null && limit != 0 && !limitsWereApplied) {
                boolean withLimit = limit > 0 && withTiesSortOrder == null;

                if (offset > 0 || withLimit)
                    sort.sort(rows, offset, withLimit ? limit : rows.size());
                else
                    sort.sort(rows);
            }
        }

        applyOffsetAndLimit();
        reset();
    }

    private void applyOffsetAndLimit() {
        if (limitsWereApplied) {
            return;
        }
        int offset = Math.max(this.offset, 0);
        int limit = this.limit;
        if (offset == 0 && limit < 0 && !fetchPercent || rowCount == 0) {
            return;
        }
        if (fetchPercent) {
            if (limit < 0 || limit > 100) {
                throw DbException.getInvalidValueException("FETCH PERCENT", limit);
            }
            // Oracle rounds percent up, do the same for now
            limit = (int) (((long) limit * rowCount + 99) / 100);
        }
        boolean clearAll = offset >= rowCount || limit == 0;
        if (!clearAll) {
            int remaining = rowCount - offset;
            limit = limit < 0 ? remaining : Math.min(remaining, limit);
            if (offset == 0 && remaining <= limit) {
                return;
            }
        } else {
            limit = 0;
        }
        distinctRows = null;
        rowCount = limit;
        if (external == null) {
            if (clearAll) {
                rows.clear();
                return;
            }
            int to = offset + limit;
            if (withTiesSortOrder != null) {
                Value[] expected = rows.get(to - 1);
                while (to < rows.size() && withTiesSortOrder.compare(expected, rows.get(to)) == 0) {
                    to++;
                    rowCount++;
                }
            }
            if (offset != 0 || to != rows.size()) {
                // avoid copying the whole array for each row
                rows = new ArrayList<>(rows.subList(offset, to));
            }
        } else {
            if (clearAll) {
                external.close();
                external = null;
                return;
            }
            trimExternal(offset, limit);
        }
    }

    /**
     * @param offset Offset.
     * @param limit Limit.
     */
    private void trimExternal(int offset, int limit) {
        ResultExternal temp = external;
        external = null;

        temp.reset();

        while (--offset >= 0)
            temp.next();

        Value[] row = null;

        while (--limit >= 0) {
            row = temp.next();
            rows.add(row);

            if (!hasAvailableMemory(null,null, row))
                addRowsToDisk(true);
        }
        if (withTiesSortOrder != null && row != null) {
            Value[] expected = row;

            while ((row = temp.next()) != null && withTiesSortOrder.compare(expected, row) == 0) {
                rows.add(row);
                rowCount++;

                if (!hasAvailableMemory(null,null, row))
                    addRowsToDisk(true);
            }
        }

        if (external != null)
            addRowsToDisk(true);

        temp.close();
    }

    /** {@inheritDoc} */
    @Override public int getRowCount() {
        return rowCount;
    }

    /** {@inheritDoc} */
    @Override public void limitsWereApplied() {
        this.limitsWereApplied = true;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return !closed && rowId < rowCount - 1;
    }

    /** {@inheritDoc} */
    @Override public void setLimit(int limit) {
        this.limit = limit;
    }

    /** {@inheritDoc} */
    @Override public void setFetchPercent(boolean fetchPercent) {
        this.fetchPercent = fetchPercent;
    }

    /** {@inheritDoc} */
    @Override public void setWithTies(SortOrder withTiesSortOrder) {
        assert sort == null || sort == withTiesSortOrder;
        this.withTiesSortOrder = withTiesSortOrder;
    }

    /** {@inheritDoc} */
    @Override public boolean needToClose() {
        return !closed;
    }

    /** */
    public long memoryReserved() {
        return memReserved;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (!closed) {
            onClose();

            if (external != null) {
                external.close();
                external = null;
            }

            closed = true;
        }
    }

    /** {@inheritDoc} */
    @Override public String getAlias(int i) {
        return expressions[i].getAlias();
    }

    /** {@inheritDoc} */
    @Override public String getTableName(int i) {
        return expressions[i].getTableName();
    }

    /** {@inheritDoc} */
    @Override public String getSchemaName(int i) {
        return expressions[i].getSchemaName();
    }

    /** {@inheritDoc} */
    @Override public String getColumnName(int i) {
        return expressions[i].getColumnName();
    }

    /** {@inheritDoc} */
    @Override public TypeInfo getColumnType(int i) {
        return expressions[i].getType();
    }

    /** {@inheritDoc} */
    @Override public int getNullable(int i) {
        return expressions[i].getNullable();
    }

    /** {@inheritDoc} */
    @Override public boolean isAutoIncrement(int i) {
        return expressions[i].isAutoIncrement();
    }

    /** {@inheritDoc} */
    @Override public void setOffset(int offset) {
        this.offset = offset;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return super.toString() + " columns: " + visibleColumnCount +
            " rows: " + rowCount + " pos: " + rowId;
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public int getFetchSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void setFetchSize(int fetchSize) {
        // ignore
    }

    /**
     * @return Memory tracker.
     */
    public H2MemoryTracker memoryTracker() {
        return memTracker;
    }

    /** Close event handler. */
    protected void onClose() {
        // Allow results to be collected by GC before mark memory released.
        distinctRows = null;
        rows = null;

        if (memReserved > 0) {
            H2MemoryTracker tracker = session.memoryTracker();

            assert tracker != null;

            tracker.release(memReserved);

            memReserved = 0;
        }
    }

    /** */
    private Map<Value, Value[]> createDistinctMap() {
        boolean useTreeMap = USE_TREEMAP;

        if (!useTreeMap) {
            if (distinctIndexes != null) {
                for (int i : distinctIndexes) {
                    if (expressions[i].getType().getValueType() == Value.DECIMAL) {
                        useTreeMap = true;

                        break;
                    }
                }
            }
            else
                useTreeMap = Arrays.stream(expressions).anyMatch(e -> e.getType().getValueType() == Value.DECIMAL);
        }

        return useTreeMap ? new TreeMap<>(session.getDatabase().getCompareMode()) : new HashMap<>();
    }
}

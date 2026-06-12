/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.gridgain.internal.h2.index;

import java.util.ArrayList;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.expression.Parameter;
import org.gridgain.internal.h2.expression.condition.Comparison;
import org.gridgain.internal.h2.message.DbException;
import org.gridgain.internal.h2.result.ResultInterface;
import org.gridgain.internal.h2.result.Row;
import org.gridgain.internal.h2.result.SearchRow;
import org.gridgain.internal.h2.result.SortOrder;
import org.gridgain.internal.h2.table.Column;
import org.gridgain.internal.h2.table.IndexColumn;
import org.gridgain.internal.h2.table.Table;
import org.gridgain.internal.h2.table.TableFilter;
import org.gridgain.internal.h2.value.Value;
import org.gridgain.internal.h2.value.ValueGeometry;
import org.gridgain.internal.h2.value.ValueNull;

/**
 * The filter used to walk through an index. This class supports IN(..)
 * and IN(SELECT ...) optimizations.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class IndexCursor implements Cursor, AutoCloseable {

    private final TableFilter tableFilter;
    private Index index;
    private Table table;
    private IndexColumn[] indexColumns;
    private boolean alwaysFalse;

    private SearchRow start, end, intersects;
    private Cursor cursor;
    private Column inColumn;
    private int inListIndex;
    private Value[] inList;
    private ResultInterface inResult;

    /**
     * Separate upper bound used while expanding an IN(...) list into per-value index scans
     * when a range condition is present on a trailing index column. When non-null,
     * find(Value) performs a range scan [start, inRangeEnd] per value instead of collapsing
     * to a point lookup [start, start].
     *
     * <p>For example {@code SELECT * FROM t WHERE f1=X AND f2 IN (Y1, Y2) AND f3 > Z}
     * with index {@code IDX(f1,f2,f3)} can be expanded into union of 2 range scans:
     * <ol>
     *     <li>{@code IDX.find(f1=X, f2=Y1, f3=Z)}</li>
     *     <li>{@code IDX.find(f1=X, f2=Y1, f3=null)}</li>
     * </ol>
     */
    private SearchRow inRangeEnd;

    public IndexCursor(TableFilter filter) {
        this.tableFilter = filter;
    }

    public void setIndex(Index index) {
        this.index = index;
        this.table = index.getTable();
        Column[] columns = table.getColumns();
        indexColumns = new IndexColumn[columns.length];
        IndexColumn[] idxCols = index.getIndexColumns();
        if (idxCols != null) {
            for (int i = 0, len = columns.length; i < len; i++) {
                int idx = index.getColumnIndex(columns[i]);
                if (idx >= 0) {
                    indexColumns[i] = idxCols[idx];
                }
            }
        }
    }

    /**
     * Prepare this index cursor to make a lookup in index.
     *
     * @param s Session.
     * @param indexConditions Index conditions.
     */
    public void prepare(Session s, ArrayList<IndexCondition> indexConditions) {
        alwaysFalse = false;
        start = end = null;
        inList = null;
        inColumn = null;
        inResult = null;
        intersects = null;
        inRangeEnd = null;

        boolean onlyEqualityConditionsSoFar = true;

        // Bitmask of index column positions constrained by an equality (constant/parameter).
        // Used to decide whether an IN list can be expanded even when a range condition is
        // present on a trailing index column (see canExpandInWithTrailingRange).
        long eqPrefixMask = 0L;

        for (IndexCondition condition : indexConditions) {
            if (condition.isAlwaysFalse()) {
                alwaysFalse = true;
                break;
            }
            // If index can perform only full table scan do not try to use it for regular
            // lookups, each such lookup will perform an own table scan.
            if (index.isFindUsingFullTableScan()) {
                continue;
            }

            Column column = condition.getColumn();
            if (condition.getCompareType() == Comparison.IN_LIST) {
                // Always record the IN column candidate, regardless of the other conditions seen so far.
                // Index conditions are not ordered, so a range and/or the equality prefix that make
                // a per-value expansion legal may be processed before or after this IN.

                // We can handle only one IN(...) index scan: keep the earliest index column.
                if (inColumn != null && index.getColumnIndex(inColumn) <= index.getColumnIndex(column))
                    continue;

                useInListExpansion(column, condition.getCurrentValueList(s));
            }
            else if (condition.getCompareType() == Comparison.IN_QUERY) {
                // Always record the IN(subquery) column candidate, mirroring IN_LIST logic.
                // Index conditions are not ordered, so a range and/or the equality prefix that make
                // a per-value expansion legal may be processed before or after this IN.

                // We can handle only one IN(...) index scan: keep the earliest index column.
                if (inColumn != null && index.getColumnIndex(inColumn) <= index.getColumnIndex(column))
                    continue;

                useInQueryExpansion(column, condition.getCurrentResult());
            }
            else {
                boolean isEquality = condition.getCompareType() == Comparison.EQUAL &&
                    (condition.getExpression().isConstant() || condition.getExpression() instanceof Parameter);

                if (isEquality) {
                    int idxPos = index.getColumnIndex(column);

                    if (idxPos >= 0 && idxPos < Long.SIZE)
                        eqPrefixMask |= 1L << idxPos;
                }

                onlyEqualityConditionsSoFar &= isEquality;

                Value v = condition.getCurrentValue(s);
                boolean isStart = condition.isStart();
                boolean isEnd = condition.isEnd();
                boolean isIntersects = condition.isSpatialIntersects();
                int columnId = column.getColumnId();
                if (columnId != SearchRow.ROWID_INDEX) {
                    IndexColumn idxCol = indexColumns[columnId];
                    if (idxCol != null && (idxCol.sortType & SortOrder.DESCENDING) != 0) {
                        // if the index column is sorted the other way, we swap
                        // end and start NULLS_FIRST / NULLS_LAST is not a
                        // problem, as nulls never match anyway
                        boolean temp = isStart;
                        isStart = isEnd;
                        isEnd = temp;
                    }
                }
                if (isStart) {
                    start = table.getSearchRow(start, columnId, v, true);
                }
                if (isEnd) {
                    end = table.getSearchRow(end, columnId, v, false);
                }
                if (isIntersects) {
                    intersects = getSpatialSearchRow(intersects, columnId, v);
                }
            }
        }

        // A range condition is present (onlyEqualityConditionsSoFar == false) and an IN column was
        // recorded. Decide how to use the IN list/query against the index.
        if (inColumn != null && !onlyEqualityConditionsSoFar && (start != null || end != null)) {

            if (start != null && end != null && canExpandInWithTrailingRange(inColumn, eqPrefixMask)) {
                // The equality prefix plus a fixed IN value form an exact index prefix and the only
                // remaining (range) conditions are on trailing index columns.
                //
                // Keep the IN expansion: each value from IN (...) list, is mapped to one
                // range scan [start, inRangeEnd]  with the value fixed in both bounds
                // and the trailing range left open,  instead of a single wide scan
                // with the IN applied as a post-filter. Covers both a non-leading IN with an equality prefix
                // and a leading IN (empty prefix).
                inRangeEnd = end;
            }
            else if (!isFirstIndexOrViewColumn(inColumn)) {
                // Non-leading IN that cannot form an exact prefix.
                // An X=? condition produces fewer rows than X IN(..).
                dropInExpansion();
            }

            // A leading IN we could not set up as a range scan; the reset below handles it.
        }

        if (inColumn == null)
            return;

        if (start == null || (!onlyEqualityConditionsSoFar && isFirstIndexOrViewColumn(inColumn) && inRangeEnd == null)) {
            // Clear the accumulated bound and the range becomes a post-filter.
            start = table.getTemplateRow();
        }
    }

    private void useInListExpansion(Column column, Value[] inListValues) {
        updateInParts(column, inListValues, null);
    }

    private void useInQueryExpansion(Column column, ResultInterface inQueryResult) {
        updateInParts(column, null, inQueryResult);
    }

    private void dropInExpansion() {
        updateInParts(null, null, null);
    }

    private void updateInParts(Column column, Value[] inListValues, ResultInterface inQueryResult) {
        inColumn = column;

        inList = inListValues;
        inListIndex = 0;

        if (inResult != null)
            inResult.close();

        inResult = inQueryResult;
    }

    /**
     * Re-evaluate the start and end values of the index search for rows.
     *
     * @param s the session
     * @param indexConditions the index conditions
     */
    public void find(Session s, ArrayList<IndexCondition> indexConditions) {
        prepare(s, indexConditions);
        if (inColumn != null) {
            return;
        }
        if (!alwaysFalse) {
            if (intersects != null && index instanceof SpatialIndex) {
                cursor = ((SpatialIndex) index).findByGeometry(tableFilter,
                        start, end, intersects);
            } else if (index != null) {
                cursor = index.find(tableFilter, start, end);
            }
        }
    }

    /**
     * Checks whether an IN(...) condition on the given column can be expanded into a separate
     * index range scan per value while a range condition exists on a trailing index column.
     * For example {@code SELECT * FROM t WHERE f1=X AND f2 IN (Y1, Y2, ..) AND f3 > Z}
     *
     * <p>This is safe when every index column before the IN column is constrained by an equality
     * (a leading IN column has an empty prefix and qualifies trivially). Then the equality prefix
     * plus a fixed IN value form an exact index prefix, and the trailing range can be applied as
     * the open part of each per-value scan without missing rows.
     *
     * @param column the IN column.
     * @param eqPrefixMask bitmask of index column positions constrained by an equality.
     * @return {@code true} if the IN list can be expanded together with a trailing range.
     * @see #inRangeEnd
     */
    private boolean canExpandInWithTrailingRange(Column column, long eqPrefixMask) {
        if (column == null)
            return false;

        int inIdx = index.getColumnIndex(column);

        // Unknown/too-far positions are skipped. A leading IN column (inIdx == 0) has an empty
        // prefix, so the check below trivially holds and it is always expandable.
        if (inIdx < 0 || inIdx >= Long.SIZE)
            return false;

        // All index columns before the IN column must be pinned by an equality.
        long prefixBitMask = (1L << inIdx) - 1;

        return (eqPrefixMask & prefixBitMask) == prefixBitMask;
    }

    private boolean isFirstIndexOrViewColumn(Column column) {
        // The first column of the index must match this column,
        // or it must be a VIEW index (where the column is null).
        // Multiple IN conditions with views are not supported, see
        // IndexCondition.getMask.
        IndexColumn[] cols = index.getIndexColumns();
        if (cols == null) {
            return true;
        }
        IndexColumn idxCol = cols[0];
        return idxCol == null || idxCol.column == column;
    }

    private SearchRow getSpatialSearchRow(SearchRow row, int columnId, Value v) {
        if (row == null) {
            row = table.getTemplateRow();
        } else if (row.getValue(columnId) != null) {
            // if an object needs to overlap with both a and b,
            // then it needs to overlap with the union of a and b
            // (not the intersection)
            ValueGeometry vg = (ValueGeometry) row.getValue(columnId).
                    convertTo(Value.GEOMETRY);
            v = ((ValueGeometry) v.convertTo(Value.GEOMETRY)).
                    getEnvelopeUnion(vg);
        }
        if (columnId == SearchRow.ROWID_INDEX) {
            row.setKey(v.getLong());
        } else {
            row.setValue(columnId, v);
        }
        return row;
    }

    /**
     * Check if the result is empty for sure.
     *
     * @return true if it is
     */
    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

    /**
     * Get start search row.
     *
     * @return search row
     */
    public SearchRow getStart() {
        return start;
    }

    /**
     * Get end search row.
     *
     * @return search row
     */
    public SearchRow getEnd() {
        return end;
    }

    @Override
    public Row get() {
        if (cursor == null) {
            return null;
        }
        return cursor.get();
    }

    @Override
    public SearchRow getSearchRow() {
        return cursor.getSearchRow();
    }

    @Override
    public boolean next() {
        while (true) {
            if (cursor == null) {
                nextCursor();
                if (cursor == null) {
                    return false;
                }
            }
            if (cursor.next()) {
                return true;
            }
            cursor = null;
        }
    }

    private void nextCursor() {
        if (inList != null) {
            while (inListIndex < inList.length) {
                Value v = inList[inListIndex++];
                if (v != ValueNull.INSTANCE) {
                    find(v);
                    break;
                }
            }
        } else if (inResult != null) {
            while (inResult.next()) {
                Value v = inResult.currentRow()[0];
                if (v != ValueNull.INSTANCE) {
                    find(v);
                    break;
                }
            }
        }
    }

    private void find(Value v) {
        v = inColumn.convert(v);
        int id = inColumn.getColumnId();
        start.setValue(id, v);

        if (inRangeEnd != null) {
            // IN expansion combined with a trailing range: fix the value in both bounds and let
            // the trailing range columns define the open part of the per-value range scan.
            inRangeEnd.setValue(id, v);
            cursor = index.find(tableFilter, start, inRangeEnd);
        } else
            cursor = index.find(tableFilter, start, start);
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public void close() throws Exception {
        if (inResult != null)
            inResult.close();

        if (cursor instanceof AutoCloseable)
            ((AutoCloseable)cursor).close();
    }
}

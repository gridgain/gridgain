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

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.util.typedef.T2;
import org.h2.command.dml.GroupByData;
import org.h2.engine.Session;
import org.h2.expression.aggregate.AggregateData;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueRow;

import static org.h2.command.dml.SelectGroups.cleanupAggregates;

/**
 * Group by data with disk offload capabilities.
 */
public class ManagedGroupByData extends GroupByData {
    /** Indexes of group-by columns. */
    private final int[] grpIdx;

    /** External group-by result (offloaded groups). */
    private GroupedExternalResult sortedExtRes;

    /** In-memory buffer for groups. */
    private TreeMap<ValueRow, Object[]> groupByData;

    /** */
    private ValueRow lastGrpKey;

    /** */
    private Object[] lastGrpData;

    /** */
    private Iterator<T2<ValueRow, Object[]>> cursor;

    /** */
    Map.Entry<ValueRow, Object[]> curEntry;

    /** */
    private int size;

    /**
     * @param ses Session.
     * @param grpIdx Indexes of group-by columns.
     */
    public ManagedGroupByData(Session ses, int[] grpIdx) {
        super(ses);

        this.grpIdx = grpIdx;

        groupByData = new TreeMap<>(ses.getDatabase().getCompareMode());
    }

    /** */
    private void createExtGroupByData() {
        sortedExtRes = new GroupedExternalResult(((QueryContext)ses.getQueryContext()).context(), ses,
             tracker,  groupByData.size());
    }

    /** {@inheritDoc} */
    @Override public Object[] nextSource(ValueRow grpKey, int width) {
        lastGrpKey = grpKey;

        lastGrpData = groupByData.get(grpKey);

        if (lastGrpData == null) {
            lastGrpData = new Object[width];

            groupByData.put(grpKey, lastGrpData);

            onGroupChanged(grpKey, null, lastGrpData);

            size++;
        }

        return lastGrpData;
    }

    /** {@inheritDoc} */
    @Override public long size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public boolean next() {
        assert cursor != null;

        boolean hasNext = cursor.hasNext();

        curEntry = hasNext ? cursor.next() : null;

        return hasNext;
    }

    /** {@inheritDoc} */
    @Override public ValueRow groupKey() {
        assert curEntry != null;

        return curEntry.getKey();
    }

    /** {@inheritDoc} */
    @Override public Object[] groupByExprData() {
        assert curEntry != null;

        return curEntry.getValue();
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        if (sortedExtRes != null) {
            sortedExtRes.close();

            sortedExtRes = null;
        }

        cursor = null;
        sortedExtRes = null;
        groupByData = new TreeMap<>(ses.getDatabase().getCompareMode());
        lastGrpKey = null;

        curEntry = null;
        tracker.released(memReserved);
        memReserved = 0;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    /** {@inheritDoc} */
    @Override public void onRowProcessed() {
        Object[] old = groupByData.put(lastGrpKey, lastGrpData);

        onGroupChanged(lastGrpKey, old, lastGrpData);

        if (!tracker.reserved(0)) {
            if (sortedExtRes == null)
                createExtGroupByData();

            spillGroupsToDisk();
        }
    }

    /**
     * Does the actual disk spilling.
     */
    private void spillGroupsToDisk() {
        sortedExtRes.spillGroupsToDisk(groupByData);

        for (Map.Entry<ValueRow, Object[]> row : groupByData.entrySet())
            cleanupAggregates(row.getValue(), ses);

        groupByData.clear();

        tracker.released(memReserved);

        memReserved = 0;
    }

    /** {@inheritDoc} */
    @Override public void updateCurrent(Object[] grpByExprData) {
        // Looks like group-by data size can be increased only on the very first group update.
        // What is the sense of having groups with the different aggregate arrays sizes?
        assert size == 1 : "size=" + size;
        assert sortedExtRes == null;

        Object[] old = groupByData.put(lastGrpKey, grpByExprData);

        onGroupChanged(lastGrpKey, old, grpByExprData);
    }

    /** {@inheritDoc} */
    @Override public void done(int width) {
        if (grpIdx == null && sortedExtRes == null && groupByData.isEmpty())
            groupByData.put(ValueRow.getEmpty(), new Object[width]);


        if (sortedExtRes != null ) {

            if (!groupByData.isEmpty())
                spillGroupsToDisk();

            sortedExtRes.reset();

            cursor = new ExternalGroupsIterator(sortedExtRes, ses);
        }
        else
            cursor = new GroupsIterator(groupByData.entrySet().iterator());
    }

    /**
     * Iterator over in-memory groups.
     */
    private static class GroupsIterator implements Iterator<T2<ValueRow, Object[]>> {
        /**  */
        private final Iterator<Map.Entry<ValueRow, Object[]>> it;

        /**
         * @param it In-memory groups iterator.
         */
        private GroupsIterator(Iterator<Map.Entry<ValueRow, Object[]>> it) {
            this.it = it;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override public T2<ValueRow, Object[]> next() {
            Map.Entry<ValueRow, Object[]> row = it.next();

            return new T2<>(row.getKey(), row.getValue());
        }
    }

    /**
     * Iterator over offloaded (spilled) groups.
     */
    private static class ExternalGroupsIterator implements Iterator<T2<ValueRow, Object[]>> {
        /** */
        private final GroupedExternalResult sortedExtRes;

        /** */
        private final CompareMode cmp;

        /** */
        private final Session ses;

        /** */
        private int extSize;

        /** */
        private T2<ValueRow, Object[]> cur;

        /** */
        private T2<ValueRow, Object[]> next;

        /**
         * @param res External result.
         * @param ses Session.
         */
        private ExternalGroupsIterator(GroupedExternalResult res, Session ses) {
            sortedExtRes = res;
            this.ses = ses;
            this.cmp = ses.getDatabase().getCompareMode();
            extSize = sortedExtRes.size();
            advance();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return cur != null;
        }

        /** {@inheritDoc} */
        @Override public T2<ValueRow, Object[]> next() {
            if (cur == null)
                throw new NoSuchElementException();

            T2<ValueRow, Object[]> res = cur;
            cur = next;
            next = null;
            advance();

            return res;
        }

        /**
         * Moves cursor forward.
         */
        private void advance() {
            assert next == null;

            while (extSize-- > 0) {
                Object[] row = sortedExtRes.next();

                if (row == null)
                    break;

                if (cur == null) {
                    cur = getEntry(row);

                    continue;
                }

                if (cur.getKey().compareTypeSafe((ValueRow)row[0], cmp) == 0) {
                    Object[] curAggs = cur.getValue();

                    for (int i = 0; i < curAggs.length; i++) {
                        Object newAgg = row[i + 1];
                        Object curAgg = curAggs[i];

                        mergeAggregates(curAgg, newAgg, ses);
                    }
                }
                else {
                    next = getEntry(row);

                    break;
                }
            }
        }

        /** */
        T2<ValueRow, Object[]> getEntry(Object[] row) {
            Object[] aggs = new Object[row.length - 1];

            System.arraycopy(row, 1, aggs, 0, aggs.length);

            return new T2<>((ValueRow)row[0], aggs);
        }

        /**  */
        private static void mergeAggregates(Object curAgg, Object newAgg, Session ses) {
            assert (newAgg == null) == (curAgg == null) : "newAgg=" + newAgg + ", curAgg=" + curAgg;

            if (newAgg == null)
                return;

            assert newAgg.getClass() == curAgg.getClass() : "newAgg=" + newAgg + ", curAgg=" + curAgg;

            if (newAgg instanceof AggregateData)
                ((AggregateData)curAgg).mergeAggregate(ses, (AggregateData)newAgg);
            else if (!(newAgg instanceof Value)) // Aggregation means no-op for Value.
                throw new UnsupportedOperationException("Unsupported aggregate:" +
                    newAgg.getClass() + ", curAgg=" + curAgg.getClass());
        }
    }
}

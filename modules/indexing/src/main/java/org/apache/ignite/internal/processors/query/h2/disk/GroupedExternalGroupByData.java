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
import org.jetbrains.annotations.NotNull;

/**
 * TODO: Add class description.
 */
public class GroupedExternalGroupByData extends GroupByData {

    private final int[] grpIdx;

    private GroupedExternalResult sortedExtRes;

    private TreeMap<ValueRow, Object[]> groupByData;

    private ValueRow lastGrpKey;
    private Object[] lastGrpData;

    private Iterator<T2<ValueRow, Object[]>> cursor;

    Map.Entry<ValueRow, Object[]> curEntry;

    private int size;

    public GroupedExternalGroupByData(Session ses, int[] grpIdx) {
        super(ses);

        this.grpIdx = grpIdx;

        groupByData = new TreeMap<>(ses.getDatabase().getCompareMode());
    }

    private void createExtGroupByData() {
        sortedExtRes = new GroupedExternalResult(((QueryContext)ses.getQueryContext()).context(), ses,
             tracker,  groupByData.size());
    }

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

    @Override public long size() {
        return size;
    }

    @Override public boolean next() {
        assert cursor != null;

        boolean hasNext = cursor.hasNext();

        curEntry = hasNext ? cursor.next() : null;

        return hasNext;
    }

    @Override public ValueRow groupKey() {
        assert curEntry != null;

        return curEntry.getKey();
    }

    @Override public Object[] groupByExprData() {
        assert curEntry != null;

        return curEntry.getValue();
    }

    @Override public void cleanup() {
        // TODO Cleanup aggregates and merge with reset()?
    }

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

    @Override public void remove() {
        assert cursor != null;

        cursor.remove();

        onGroupChanged(curEntry.getKey(), curEntry.getValue(), null);

        size--;
    }

    @Override public void onRowProcessed() { // TODO exception for non-serializable aggregates.
        Object[] old = groupByData.put(lastGrpKey, lastGrpData);

        onGroupChanged(lastGrpKey, old, lastGrpData);

        if (!tracker.reserved(0)) {
            if (sortedExtRes == null)
                createExtGroupByData();

            spillGroupsToDisk();

        }

    }

    private void spillGroupsToDisk() {
        for (Map.Entry<ValueRow, Object[]> e : groupByData.entrySet()) {
            ValueRow key = e.getKey();
            Object[] aggs = e.getValue();

            Object[] newRow = getObjectsArray(key, aggs);

            //throw new RuntimeException("!!!");
            sortedExtRes.addRow(newRow);


        }

        groupByData.clear();

        tracker.released(memReserved); // TODO cleanup aggregates

        memReserved = 0;
    }

    @NotNull private Object[] getObjectsArray(ValueRow key, Object[] aggs) {
        Object[] newRow = new Object[aggs.length + 1];

        newRow[0] = key;

        System.arraycopy(aggs, 0, newRow, 1, aggs.length);
        return newRow;
    }

    @Override public void updateCurrent(Object[] grpByExprData) {
        // Looks like group-by data size can be increased only on the very first group update.
        // What is the sense of having groups with the different aggregate arrays sizes?
        assert size == 1 : "size=" + size;
        assert sortedExtRes == null;

        Object[] old = groupByData.put(lastGrpKey, grpByExprData);

        onGroupChanged(lastGrpKey, old, grpByExprData);
    }

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

    private static class GroupsIterator implements Iterator<T2<ValueRow, Object[]>> {

        private final Iterator<Map.Entry<ValueRow, Object[]>> it;

        private GroupsIterator(Iterator<Map.Entry<ValueRow, Object[]>> it) {
            this.it = it;
        }

        @Override public boolean hasNext() {
            return it.hasNext();
        }

        @Override public T2<ValueRow, Object[]> next() {
            Map.Entry<ValueRow, Object[]> row = it.next();

            return new T2<>(row.getKey(), row.getValue());
        }
    }

    private static class ExternalGroupsIterator implements Iterator<T2<ValueRow, Object[]>> {
        private final GroupedExternalResult sortedExtRes;
        private final CompareMode cmp;
        private final Session ses;
        private int extSize;
        private T2<ValueRow, Object[]> cur;
        private T2<ValueRow, Object[]> next;


        private ExternalGroupsIterator(GroupedExternalResult res, Session ses) {
            sortedExtRes = res;
            this.ses = ses;
            this.cmp = ses.getDatabase().getCompareMode();
            extSize = sortedExtRes.size();
            advance();
        }

        @Override public boolean hasNext() {
            return cur != null;
        }

        @Override public T2<ValueRow, Object[]> next() {
            if (cur == null)
                throw new NoSuchElementException();

            T2<ValueRow, Object[]> res = cur;
            cur = next;
            next = null;
            advance();

            return res;
        }

        private void advance() {
            assert next == null;

            while (extSize-- > 0) {
                Object[] row = sortedExtRes.next();

                if (cur == null) {
                    cur = getEntry(row);

                    continue;
                }

                if (cur.getKey().compareTypeSafe((ValueRow)row[0], cmp) == 0) {
                    Object[] curAggs = cur.getValue();

                    for (int i = 0; i < curAggs.length; i++) {
                        Object newAgg = row[i + 1];
                        Object curAgg = curAggs[i];

                        assert (newAgg == null) == (curAgg == null) : "newAgg=" + newAgg + ", curAgg=" + curAgg;

                        if (newAgg == null)
                            continue;

                        assert newAgg.getClass() == curAgg.getClass() : "newAgg=" + newAgg + ", curAgg=" + curAgg;

                        if (newAgg instanceof AggregateData)
                            ((AggregateData)curAgg).mergeAggregate(ses, (AggregateData)newAgg);
                        else if (!(newAgg instanceof Value)) // Aggregation means no-op for Value.
                            throw new UnsupportedOperationException("Unsupported aggregate:" +
                                newAgg.getClass() + ", curAgg=" + curAgg.getClass());
                    }
                }
                else {
                    next = getEntry(row);

                    break;
                }

            }


            // TODO: implement.
        }

        T2<ValueRow, Object[]> getEntry(Object[] row) {
            Object[] aggs = new Object[row.length - 1];

            System.arraycopy(row, 1, aggs, 0, aggs.length);

            return new T2<>((ValueRow)row[0], aggs);
        }


    }

}

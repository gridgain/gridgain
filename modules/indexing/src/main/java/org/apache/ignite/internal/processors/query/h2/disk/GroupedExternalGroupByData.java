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

import java.nio.BufferUnderflowException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.command.dml.GroupByData;
import org.h2.engine.Session;
import org.h2.value.ValueRow;

/**
 * TODO: Add class description.
 */
public class GroupedExternalGroupByData extends GroupByData {

    private final int[] grpIdx;

    private ExternalGroups extGroupByData;

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

    private void createExternalGroupByData() {
        extGroupByData = new ExternalGroups(((QueryContext)ses.getQueryContext()).context(), ses.queryMemoryTracker());
    }

    @Override public Object[] nextSource(ValueRow grpKey, int width) {
        lastGrpKey = grpKey;

        lastGrpData = groupByData.get(grpKey);

        if (lastGrpData == null && extGroupByData != null) {
            try {
                lastGrpData = extGroupByData.get(grpKey);

            }
            catch (BufferUnderflowException e) {
                extGroupByData.get(grpKey);
            }

//            if (lastGrpData != null)
//                groupByData.put(grpKey, lastGrpData);
        }

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
        if (extGroupByData != null) {
            U.closeQuiet(extGroupByData);

            extGroupByData = null;
        }

        cursor = null;
        extGroupByData = null;
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
            if (extGroupByData == null)
                createExternalGroupByData();

            spillGroupsToDisk();

        }

    }

    private void spillGroupsToDisk() {
        for (Map.Entry<ValueRow, Object[]> e : groupByData.entrySet()) {
            extGroupByData.put(e.getKey(), e.getValue());
        }

        groupByData.clear();

        tracker.released(memReserved); // TODO cleanup aggregates

        memReserved = 0;
    }

    @Override public void updateCurrent(Object[] grpByExprData) {
        // Looks like group-by data size can be increased only on the very first group update.
        // What is the sense of having groups with the different aggregate arrays sizes?
        assert size == 1 : "size=" + size;
        assert extGroupByData == null;

        Object[] old = groupByData.put(lastGrpKey, grpByExprData);

        onGroupChanged(lastGrpKey, old, grpByExprData);
    }

    @Override public void done(int width) {
        if (grpIdx == null && extGroupByData == null && groupByData.isEmpty())
            extGroupByData.put(ValueRow.getEmpty(), new Object[width]);

        if (extGroupByData != null ) {
            if (!groupByData.isEmpty())
                spillGroupsToDisk();

            cursor = extGroupByData.cursor();
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

}

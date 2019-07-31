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
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.util.typedef.T2;
import org.h2.command.dml.GroupByData;
import org.h2.engine.Session;
import org.h2.value.ValueRow;

/**
 * TODO: Add class description.
 */
public class GroupedExternalGroupByData extends GroupByData {

    private final int[] grpIdx;

    private ExternalGroups groupByData;


    //private TreeMap<ValueRow, Object[]> groupByData;


    private ValueRow lastGrpKey;
    private Object[] lastGrpData;

    private Iterator<T2<ValueRow, Object[]>> cursor;

    Map.Entry<ValueRow, Object[]> curEntry;


    public GroupedExternalGroupByData(Session ses, int[] grpIdx) {
        super(ses);

        this.grpIdx = grpIdx;
        groupByData = new ExternalGroups(((QueryContext)ses.getQueryContext()).context(), ses.queryMemoryTracker()); //new TreeMap<>(ses.getDatabase().getCompareMode());
    }

    @Override public Object[] nextSource(ValueRow grpKey, int width) {
        lastGrpKey = grpKey;

        lastGrpData = groupByData.get(grpKey);

        if (lastGrpData == null) {
            lastGrpData = new Object[width];

//            groupByData.put(grpKey, values);
//
//            onGroupChanged(grpKey, null, values);
        }

        return lastGrpData;
    }

    @Override public long size() {
        return groupByData.size();
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
        cursor = null;
        groupByData = new ExternalGroups(((QueryContext)ses.getQueryContext()).context(), ses.queryMemoryTracker()); //new TreeMap<>(ses.getDatabase().getCompareMode());
        lastGrpKey = null;

        curEntry = null;
    }

    @Override public void remove() {
        assert cursor != null;

        cursor.remove();

        onGroupChanged(curEntry.getKey(), curEntry.getValue(), null);
    }

    @Override public void onRowProcessed() {
        groupByData.put(lastGrpKey, lastGrpData);
    }

    @Override public void updateCurrent(Object[] grpByExprData) {
        groupByData.put(lastGrpKey, grpByExprData);
//        Object[] old = groupByData.put(lastGrpKey, grpByExprData);
//
//        onGroupChanged(lastGrpKey, old, grpByExprData);
    }

    @Override public void done(int width) {
//        if (grpIdx == null && groupByData.isEmpty())
//            groupByData.put(ValueRow.getEmpty(), new Object[width]); TODO Empty groups

        cursor = groupByData.cursor();
    }

}

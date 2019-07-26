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
package org.h2.command.dml;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.h2.engine.Session;
import org.h2.value.ValueRow;

/**
 * TODO: Add class description.
 */
public class GroupedGroupByData extends GroupByData {
    private final int[] grpIdx;

    private TreeMap<ValueRow, Object[]> groupByData;

    private ValueRow lastGrpKey;

    private Iterator<Map.Entry<ValueRow, Object[]>> cursor;

    Map.Entry<ValueRow, Object[]> curEntry;

    public GroupedGroupByData(Session ses, int[] grpIdx) {
        super(ses);
        this.grpIdx = grpIdx;
        groupByData = new TreeMap<>(ses.getDatabase().getCompareMode());
    }

    @Override public Object[] nextSource(ValueRow grpKey, int width) {
        lastGrpKey = grpKey;

        Object[] values = groupByData.get(grpKey);

        if (values == null) {
            values = new Object[width];

            groupByData.put(grpKey, values);

            onGroupChanged(grpKey, null, values);
        }

        return values;
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
        groupByData = new TreeMap<>(ses.getDatabase().getCompareMode());
        lastGrpKey = null;
        cursor = null;
        curEntry = null;
    }

    @Override public void remove() {
        assert cursor != null;

        cursor.remove();

        onGroupChanged(curEntry.getKey(), curEntry.getValue(), null);
    }

    @Override public void onRowProcessed() {
        // No-op.
    }

    @Override public void updateCurrent(Object[] grpByExprData) {
        Object[] old = groupByData.put(lastGrpKey, grpByExprData);

        onGroupChanged(lastGrpKey, old, grpByExprData);
    }

    @Override public void done(int width) {
        if (grpIdx == null && groupByData.isEmpty())
            groupByData.put(ValueRow.getEmpty(), new Object[width]);

        cursor = groupByData.entrySet().iterator();
    }
}

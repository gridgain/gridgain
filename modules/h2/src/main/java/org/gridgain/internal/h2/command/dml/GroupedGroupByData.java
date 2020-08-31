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
package org.gridgain.internal.h2.command.dml;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.value.ValueRow;

/**
 * In-memory group data collector for group-by expressions.
 */
public class GroupedGroupByData extends GroupByData {
    /** Group by indexes. */
    private final int[] grpIdx;

    /** Group-by data. */
    private TreeMap<ValueRow, Object[]> grpByData;

    /** */
    private ValueRow lastGrpKey;

    /** */
    private Iterator<Map.Entry<ValueRow, Object[]>> cursor;

    /** */
    private Map.Entry<ValueRow, Object[]> curEntry;

    /**
     * @param ses Session.
     * @param grpIdx Group key columns.
     */
    public GroupedGroupByData(Session ses, int[] grpIdx) {
        super(ses);
        this.grpIdx = grpIdx;
        grpByData = new TreeMap<>(ses.getDatabase().getCompareMode());
    }

    /** {@inheritDoc} */
    @Override public Object[] nextSource(ValueRow grpKey, int width) {
        lastGrpKey = grpKey;

        Object[] values = grpByData.get(grpKey);

        if (values == null) {
            values = new Object[width];

            grpByData.put(grpKey, values);

            onGroupChanged(grpKey, null, values);
        }

        return values;
    }

    /** {@inheritDoc} */
    @Override public long size() {
        return grpByData.size();
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
        grpByData = new TreeMap<>(ses.getDatabase().getCompareMode());
        lastGrpKey = null;
        cursor = null;
        curEntry = null;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        assert cursor != null;

        cursor.remove();

        onGroupChanged(curEntry.getKey(), curEntry.getValue(), null);
    }

    /** {@inheritDoc} */
    @Override public void onRowProcessed() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void updateCurrent(Object[] grpByExprData) {
        Object[] old = grpByData.put(lastGrpKey, grpByExprData);

        onGroupChanged(lastGrpKey, old, grpByExprData);
    }

    /** {@inheritDoc} */
    @Override public void done(int width) {
        if (grpIdx == null && grpByData.isEmpty())
            grpByData.put(ValueRow.getEmpty(), new Object[width]);

        cursor = grpByData.entrySet().iterator();
    }
}

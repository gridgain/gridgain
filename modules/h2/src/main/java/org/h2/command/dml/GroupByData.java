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

import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.expression.aggregate.AggregateData;
import org.h2.value.Value;
import org.h2.value.ValueRow;

/**
 * TODO: Add interface description.
 */
public abstract class GroupByData {

    protected final H2MemoryTracker tracker;

    protected final Session ses;

    /**
     * Memory reserved in bytes.
     *
     * Note: Poison value '-1' means memory tracking is disabled.
     */
    protected long memReserved;

    protected GroupByData(Session ses) {
        this.ses = ses;
        this.tracker = ses.queryMemoryTracker();

        if (tracker == null)
            memReserved = -1;
    }

    public abstract Object[] nextSource(ValueRow grpKey, int width);

    public abstract void updateCurrent(Object[] grpByExprData);


    public abstract long size();

    public abstract boolean next();

    public abstract ValueRow groupKey();

    public abstract Object[] groupByExprData();

    public abstract void cleanup();


    public abstract void done(int width);

    public abstract void reset();


    public abstract void remove();

    abstract void onRowProcessed();

    protected static boolean canSpillToDisk(Object agg) {
        assert agg != null;

        if (agg instanceof AggregateData)
            return ((AggregateData)agg).hasFixedSizeInBytes(); // Not all children of AggregateData can be spilled to disk.

        if (agg instanceof org.h2.api.Aggregate)
            return false; // We can not spill user-defined aggregates.

        assert agg instanceof Value : agg.getClass();

        return ((Value)agg).hasFixedSizeInBytes(); // At the moment values with the fixed size can be spilled to disk.
    }

    /**
     * Group result updated callback.
     *
     * @param groupKey Row key.
     * @param old Old row.
     * @param row New row.
     */
    protected void onGroupChanged(ValueRow groupKey, Object[] old, Object[] row) {
        if (!trackable())
            return;

        assert old != null || row != null;

        long size;

        // Group result changed.
        if (row != null && old != null)
            size = (row.length - old.length) * Constants.MEMORY_OBJECT;
            // New group added.
        else if (old == null) {
            size = groupKey != null ? groupKey.getMemory() : 0;
            size += Constants.MEMORY_ARRAY + row.length * Constants.MEMORY_OBJECT;
        }
        // Group removed.
        else {
            size = groupKey != null ? -groupKey.getMemory() : 0;
            size -= Constants.MEMORY_ARRAY + old.length * Constants.MEMORY_OBJECT;
        }

        if (size > 0)
            tracker.reserved(size);
        else
            tracker.released(-size);

        memReserved += size;
    }

    /**
     * @return {@code True} if memory tracker available, {@code False} otherwise.
     */
    boolean trackable() {
        assert memReserved == -1 || tracker != null;

        return memReserved != -1;
    }
}

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

import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.gridgain.internal.h2.engine.Constants;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.value.ValueRow;

/**
 * Group-by data holder.
 */
public abstract class GroupByData {
    /** */
    public H2MemoryTracker tracker;

    /** */
    protected final Session ses;

    /**
     * @param ses Session.
     */
    protected GroupByData(Session ses) {
        this.ses = ses;
        tracker = ses.memoryTracker() != null ? ses.memoryTracker().createChildTracker() : null;
    }

    /** */
    protected void initTracker() {
        if (tracker == null)
            tracker = ses.memoryTracker() != null ? ses.memoryTracker().createChildTracker() : null;
    }

    /**
     * This method is called for each new row emitted from the source. See {@link SelectGroups} javadoc.
     * @param grpKey Group key.
     * @param width Aggregates array width.
     * @return Aggregates.
     */
    public abstract Object[] nextSource(ValueRow grpKey, int width);

    /**
     * Updates current aggregates data.
     * @param grpByExprData New aggregates data.
     */
    public abstract void updateCurrent(Object[] grpByExprData);

    /**
     * @return Size.
     */
    public abstract long size();

    /**
     * @return {@code True} if has next.
     */
    public abstract boolean next();

    /**
     * @return Current group key.
     */
    public abstract ValueRow groupKey();

    /**
     * @return Current group aggregates data.
     */
    public abstract Object[] groupByExprData();

    /**
     * This method is called when gathering the groups is done and we are ready to iterate over them.
     * @param width Aggregates array width.
     */
    public abstract void done(int width);

    /**
     * Resets group by data.
     */
    public abstract void reset();

    /** */
    public abstract void remove();

    /**
     * This method is called when we finished source row processing.
     */
    public abstract void onRowProcessed();

    /**
     * Group result updated callback.
     *
     * @param groupKey Row key.
     * @param old Old row.
     * @param row New row.
     */
    protected void onGroupChanged(ValueRow groupKey, Object[] old, Object[] row) {
        initTracker();

        if (tracker == null)
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
            tracker.reserve(size);
        else if (size < 0)
            tracker.release(-size);
    }
}

/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.TreeMap;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating an aggregate that needs distinct values with
 * their counts.
 */
class AggregateDataDistinctWithCounts extends AggregateData  {

    private final boolean ignoreNulls;

    private final int maxDistinctCount;

    private TreeMap<Value, LongDataCounter> values;

    private H2MemoryTracker tracker;

    /**
     * Creates new instance of data for aggregate that needs distinct values
     * with their counts.
     *
     * @param ignoreNulls
     *            whether NULL values should be ignored
     * @param maxDistinctCount
     *            maximum count of distinct values to collect
     */
    AggregateDataDistinctWithCounts(boolean ignoreNulls, int maxDistinctCount) {
        this.ignoreNulls = ignoreNulls;
        this.maxDistinctCount = maxDistinctCount;
    }

    @Override
    void add(Session ses, Value v) {
        if (ignoreNulls && v == ValueNull.INSTANCE) {
            return;
        }
        if (values == null) {
            values = new TreeMap<>(ses.getDatabase().getCompareMode());
        }
        LongDataCounter a = values.get(v);
        if (a == null) {
            if (values.size() >= maxDistinctCount) {
                return;
            }
            a = new LongDataCounter();
            values.put(v, a);

            if (tracker == null && ses.memoryTracker() != null)
                tracker = ses.memoryTracker().createChildTracker();

            if (tracker != null) {
                long size = Constants.MEMORY_OBJECT;

                size += v.getMemory();

                tracker.reserve(size);
            }
        }
        a.count++;
    }

    @Override
    public void mergeAggregate(Session ses, AggregateData agg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMemory() {
        return tracker.reserved();
    }

    @Override
    Value getValue(Database database, int dataType) {
        return null;
    }

    /**
     * Returns map with values and their counts.
     *
     * @return map with values and their counts
     */
    TreeMap<Value, LongDataCounter> getValues() {
        return values;
    }

    /** {@inheritDoc} */
    @Override public void cleanup(Session ses) {
        if (values != null && tracker != null) {
            values = null;

            tracker.release(tracker.reserved());
        }
    }
}

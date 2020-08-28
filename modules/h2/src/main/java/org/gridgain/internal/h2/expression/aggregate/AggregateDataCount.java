/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.gridgain.internal.h2.expression.aggregate;

import org.gridgain.internal.h2.engine.Constants;
import org.gridgain.internal.h2.engine.Database;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.value.Value;
import org.gridgain.internal.h2.value.ValueLong;
import org.gridgain.internal.h2.value.ValueNull;

/**
 * Data stored while calculating a COUNT aggregate.
 */
public class AggregateDataCount extends AggregateData {

    private final boolean all;

    private long count;

    AggregateDataCount(boolean all) {
        this.all = all;
    }

    private AggregateDataCount(boolean all, long count) {
        this.all = all;
        this.count = count;
    }

    @Override
    void add(Session ses, Value v) {
        if (all || v != ValueNull.INSTANCE) {
            count++;
        }
    }

    @Override
    public void mergeAggregate(Session ses, AggregateData agg) {
        assert agg != null;
        assert agg instanceof AggregateDataCount : agg.getClass();

        count += ((AggregateDataCount)agg).count;
    }

    @Override
    Value getValue(Database database, int dataType) {
        return ValueLong.get(count).convertTo(dataType);
    }

    public boolean isAll() {
        return all;
    }

    public long count() {
        return count;
    }

    public static AggregateDataCount from(boolean all, long count) {
        return new AggregateDataCount(all, count);
    }

    @Override public long getMemory() {
        return Constants.MEMORY_OBJECT + /*long*/8 + /*bool*/1 ;
    }
}

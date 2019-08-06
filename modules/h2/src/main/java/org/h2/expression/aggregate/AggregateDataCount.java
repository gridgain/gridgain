/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.value.Value;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

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

    @Override void mergeAggregate(Session ses, AggregateData agg) {
        assert agg != null;
        assert agg instanceof AggregateDataCount : agg.getClass();

        count += ((AggregateDataCount)agg).count;
    }

    @Override
    Value getValue(Database database, int dataType) {
        return ValueLong.get(count).convertTo(dataType);
    }

    @Override public boolean hasFixedSizeInBytes() {
        return true;
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

//    @Override public byte[] toBytes() throws IOException {
//        byte[] bytes = new byte[9];
//        bytes[0] =
//        out.writeBoolean(all);
//        out.writeLong(count);
//    }

//    public static AggregateDataCount read(DataInputStream in) throws IOException {
//
//        byte[] arr;
//
//        boolean all = in.readBoolean();
//        long count = in.readLong();
//
//        return new AggregateDataCount(all, count);
//    }
}

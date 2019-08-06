/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueDouble;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating an aggregate.
 */
public class AggregateDataDefault extends AggregateData {

    private final AggregateType aggregateType;
    private final int dataType;
    private long count;
    private Value value;
    private double m2, mean;

    /**
     * @param aggregateType the type of the aggregate operation
     * @param dataType the data type of the computed result
     */
    AggregateDataDefault(AggregateType aggregateType, int dataType) {
        this.aggregateType = aggregateType;
        this.dataType = dataType;
    }

    private AggregateDataDefault(AggregateType aggregateType, int dataType, long count, double m2,
        double mean, Value value) {
        this.aggregateType = aggregateType;
        this.dataType = dataType;
        this.count = count;
        this.value = value;
        this.m2 = m2;
        this.mean = mean;
    }

    @Override
    void add(Session ses, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        count++;
        switch (aggregateType) {
            case SUM:
                if (value == null) {
                    value = v.convertTo(dataType);
                }
                else {
                    v = v.convertTo(value.getValueType());
                    value = value.add(v);
                }
                break;
            case AVG:
                if (value == null) {
                    value = v.convertTo(DataType.getAddProofType(dataType));
                }
                else {
                    v = v.convertTo(value.getValueType());
                    value = value.add(v);
                }
                break;
            case MIN:
                if (value == null || ses.getDatabase().compare(v, value) < 0) {
                    value = v;
                }
                break;
            case MAX:
                if (value == null || ses.getDatabase().compare(v, value) > 0) {
                    value = v;
                }
                break;
            case STDDEV_POP:
            case STDDEV_SAMP:
            case VAR_POP:
            case VAR_SAMP: {
                // Using Welford's method, see also
                // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
                // http://www.johndcook.com/standard_deviation.html
                double x = v.getDouble();
                if (count == 1) {
                    mean = x;
                    m2 = 0;
                }
                else {
                    double delta = x - mean;
                    mean += delta / count;
                    m2 += delta * (x - mean);
                }
                break;
            }
            case EVERY:
                v = v.convertTo(Value.BOOLEAN);
                if (value == null) {
                    value = v;
                }
                else {
                    value = ValueBoolean.get(value.getBoolean() && v.getBoolean());
                }
                break;
            case ANY:
                v = v.convertTo(Value.BOOLEAN);
                if (value == null) {
                    value = v;
                }
                else {
                    value = ValueBoolean.get(value.getBoolean() || v.getBoolean());
                }
                break;
            case BIT_AND:
                if (value == null) {
                    value = v.convertTo(dataType);
                }
                else {
                    value = ValueLong.get(value.getLong() & v.getLong()).convertTo(dataType);
                }
                break;
            case BIT_OR:
                if (value == null) {
                    value = v.convertTo(dataType);
                }
                else {
                    value = ValueLong.get(value.getLong() | v.getLong()).convertTo(dataType);
                }
                break;
            default:
                DbException.throwInternalError("type=" + aggregateType);
        }
    }

    @Override void mergeAggregate(Session ses, AggregateData agg) {
        assert agg != null;
        assert agg instanceof AggregateDataDefault : agg.getClass();
        assert ((AggregateDataDefault)agg).aggregateType == aggregateType : "this=" + aggregateType +
            ", other=" + ((AggregateDataDefault)agg).aggregateType;

        AggregateDataDefault a = (AggregateDataDefault)agg;

        Value v = a.value;

        if (v == ValueNull.INSTANCE || a.count == 0) {
            return;
        }

        count += a.count;

        switch (aggregateType) {
            case SUM:
                if (value == null) {
                    value = v.convertTo(dataType);
                }
                else {
                    v = v.convertTo(value.getValueType());
                    value = value.add(v);
                }
                break;
            case AVG:
                if (value == null) {
                    value = v.convertTo(DataType.getAddProofType(dataType));
                }
                else {
                    v = v.convertTo(value.getValueType());
                    value = value.add(v);
                }
                break;
            case MIN:
                if (value == null || ses.getDatabase().compare(v, value) < 0) {
                    value = v;
                }
                break;
            case MAX:
                if (value == null || ses.getDatabase().compare(v, value) > 0) {
                    value = v;
                }
                break;
            case STDDEV_POP:
            case STDDEV_SAMP:
            case VAR_POP:
            case VAR_SAMP: {
                // Using Parallel algorithm, see also
                // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
                double avgA = mean;
                double avgB = a.mean;
                long cntA = count - a.count;
                long cntB = a.count;
                double mA = m2;
                double mB = a.m2;

                if (count == 1) {
                    mean = avgB;
                    m2 = 0;
                }
                else {
                    mean = (avgA * cntA + avgB * cntB) / (cntA + cntB);
                    double delta = avgB - avgA;
                    m2 = mA + mB + delta * delta * cntA * cntB / (cntA + cntB);
                }

                break;
            }
            case EVERY:
                v = v.convertTo(Value.BOOLEAN);
                if (value == null) {
                    value = v;
                }
                else {
                    value = ValueBoolean.get(value.getBoolean() && v.getBoolean());
                }
                break;
            case ANY:
                v = v.convertTo(Value.BOOLEAN);
                if (value == null) {
                    value = v;
                }
                else {
                    value = ValueBoolean.get(value.getBoolean() || v.getBoolean());
                }
                break;
            case BIT_AND:
                if (value == null) {
                    value = v.convertTo(dataType);
                }
                else {
                    value = ValueLong.get(value.getLong() & v.getLong()).convertTo(dataType);
                }
                break;
            case BIT_OR:
                if (value == null) {
                    value = v.convertTo(dataType);
                }
                else {
                    value = ValueLong.get(value.getLong() | v.getLong()).convertTo(dataType);
                }
                break;
            default:
                DbException.throwInternalError("type=" + aggregateType);
        }
    }

    @Override
    Value getValue(Database database, int dataType) {
        Value v = null;
        switch (aggregateType) {
            case SUM:
            case MIN:
            case MAX:
            case BIT_OR:
            case BIT_AND:
            case ANY:
            case EVERY:
                v = value;
                break;
            case AVG:
                if (value != null) {
                    v = divide(value, count);
                }
                break;
            case STDDEV_POP: {
                if (count < 1) {
                    return ValueNull.INSTANCE;
                }
                v = ValueDouble.get(Math.sqrt(m2 / count));
                break;
            }
            case STDDEV_SAMP: {
                if (count < 2) {
                    return ValueNull.INSTANCE;
                }
                v = ValueDouble.get(Math.sqrt(m2 / (count - 1)));
                break;
            }
            case VAR_POP: {
                if (count < 1) {
                    return ValueNull.INSTANCE;
                }
                v = ValueDouble.get(m2 / count);
                break;
            }
            case VAR_SAMP: {
                if (count < 2) {
                    return ValueNull.INSTANCE;
                }
                v = ValueDouble.get(m2 / (count - 1));
                break;
            }
            default:
                DbException.throwInternalError("type=" + aggregateType);
        }
        return v == null ? ValueNull.INSTANCE : v.convertTo(dataType);
    }

    private static Value divide(Value a, long by) {
        if (by == 0) {
            return ValueNull.INSTANCE;
        }
        int type = Value.getHigherOrder(a.getValueType(), Value.LONG);
        Value b = ValueLong.get(by).convertTo(type);
        a = a.convertTo(type).divide(b);
        return a;
    }

    @Override public boolean hasFixedSizeInBytes() {
        return value.hasFixedSizeInBytes();
    }

    public AggregateType aggregateType() {
        return aggregateType;
    }

    public int dataType() {
        return dataType;
    }

    public long count() {
        return count;
    }

    public Value value() {
        return value;
    }

    public double mean() {
        return mean;
    }

    public double m2() {
        return m2;
    }

    public static AggregateDataDefault from(AggregateType aggregateType, int dataType, long count, double m2,
        double mean, Value value) {
        return new AggregateDataDefault(aggregateType, dataType, count, m2, mean, value);
    }

    //    @Override public void toBytes(DataOutputStream out) throws IOException {
////        private final AggregateType aggregateType;
////        private final int dataType;
////        private long count;
////        private Value value;
////        private double m2, mean; WriteBuffer
//
//        out.writeInt(aggregateType.ordinal());
//        out.writeInt(dataType);
//        out.writeLong(count);
//        out.writeDouble(m2);
//        out.writeDouble(mean);
//
//        int dataLen = value instanceof ValueDecimal ? 100 : value.getMemory() + 4;
//
//        Data data = Data.create(null, dataLen, false);
//
//        data.writeValue(value);
//
//        out.writeInt(dataLen);
//
//        out.write(data.getBytes(), 0, dataLen);
//
//    }

//    public static Object read(DataInputStream in) throws IOException {
//        int aggTypeOrdinal = in.readInt();
//        AggregateType aggType = AggregateType.values()[aggTypeOrdinal];
//
//        int dataType = in.readInt();
//        long cnt0 = in.readLong();
//        double m2 = in.readDouble();
//        double mean = in.readDouble();
//
//        int valLen = in.readInt();
//
//        byte[] valBytes = new byte[valLen];
//
//        in.read(valBytes, 0, valLen);
//
//        Data data = Data.create(null, valBytes, false);
//
//        Value val = data.readValue();
//
//        return new AggregateDataDefault(aggType, dataType, cnt0, m2, mean, val);  // TODO: implement.
//
//    }
}

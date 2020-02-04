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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Various JDBC utility methods.
 */
public class JdbcUtils {
    /**
     * @param writer Binary writer.
     * @param items Query results items.
     * @param protoCtx Protocol context.
     */
    public static void writeItems(BinaryWriterExImpl writer, List<List<Object>> items,
        JdbcProtocolContext protoCtx) {
        writer.writeInt(items.size());

        for (List<Object> row : items) {
            if (row != null) {
                writer.writeInt(row.size());

                for (Object obj : row)
                    writeObject(writer, obj, false, protoCtx);
            }
        }
    }

    /**
     * @param reader Binary reader.
     * @param protoCtx Protocol context.
     * @return Query results items.
     */
    public static List<List<Object>> readItems(BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx) {
        int rowsSize = reader.readInt();

        if (rowsSize > 0) {
            List<List<Object>> items = new ArrayList<>(rowsSize);

            for (int i = 0; i < rowsSize; ++i) {
                int colsSize = reader.readInt();

                List<Object> col = new ArrayList<>(colsSize);

                for (int colCnt = 0; colCnt < colsSize; ++colCnt)
                    col.add(readObject(reader, false, protoCtx));

                items.add(col);
            }

            return items;
        } else
            return Collections.emptyList();
    }

    /**
     * @param writer Binary writer.
     * @param lst List to write.
     */
    public static void writeStringCollection(BinaryWriterExImpl writer, Collection<String> lst) {
        if (lst == null)
            writer.writeInt(0);
        else {
            writer.writeInt(lst.size());

            for (String s : lst)
                writer.writeString(s);
        }
    }

    /**
     * @param reader Binary reader.
     * @return List of string.
     */
    public static List<String> readStringList(BinaryReaderExImpl reader) {
        int size = reader.readInt();

        if (size > 0) {
            List<String> lst = new ArrayList<>(size);

            for (int i = 0; i < size; ++i)
                lst.add(reader.readString());

            return lst;
        }
        else
            return Collections.emptyList();
    }

    /**
     * Read nullable Integer.
     *
     * @param reader Binary reader.
     * @return read value.
     */
    @Nullable public static Integer readNullableInteger(BinaryReaderExImpl reader) {
        return reader.readBoolean() ? reader.readInt() : null;
    }

    /**
     * Write nullable integer.
     *
     * @param writer Binary writer.
     * @param val Integer value..
     */
    public static void writeNullableInteger(BinaryWriterExImpl writer, @Nullable Integer val) {
        writer.writeBoolean(val != null);

        if (val != null)
            writer.writeInt(val);
    }

    /**
     * Write nullable integer.
     *
     * @param writer Binary writer.
     * @param val Integer value..
     */
    public static void writeNullableLong(BinaryWriterExImpl writer, @Nullable Long val) {
        writer.writeBoolean(val != null);

        if (val != null)
            writer.writeLong(val);
    }

    /**
     * @param reader Reader.
     * @param binObjAllow Allow to read non plaint objects.
     * @param protoCtx Protocol context.
     * @return Read object.
     * @throws BinaryObjectException On error.
     */
    @Nullable public static Object readObject(BinaryReaderExImpl reader, boolean binObjAllow,
        JdbcProtocolContext protoCtx)
        throws BinaryObjectException {
        byte type = reader.readByte();

        switch (type) {
            case GridBinaryMarshaller.NULL:
                return null;

            case GridBinaryMarshaller.BOOLEAN:
                return reader.readBoolean();

            case GridBinaryMarshaller.BYTE:
                return reader.readByte();

            case GridBinaryMarshaller.CHAR:
                return reader.readChar();

            case GridBinaryMarshaller.SHORT:
                return reader.readShort();

            case GridBinaryMarshaller.INT:
                return reader.readInt();

            case GridBinaryMarshaller.LONG:
                return reader.readLong();

            case GridBinaryMarshaller.FLOAT:
                return reader.readFloat();

            case GridBinaryMarshaller.DOUBLE:
                return reader.readDouble();

            case GridBinaryMarshaller.STRING:
                return BinaryUtils.doReadString(reader.in());

            case GridBinaryMarshaller.DECIMAL:
                return BinaryUtils.doReadDecimal(reader.in());

            case GridBinaryMarshaller.UUID:
                return BinaryUtils.doReadUuid(reader.in());

            case GridBinaryMarshaller.DATE: {
                if (protoCtx.client()) {
                    return new java.sql.Date(convertWithTimeZone(BinaryUtils.doReadDate(reader.in()),
                        protoCtx.serverTimeZone(), TimeZone.getDefault()).getTime());
                }
                else
                    return BinaryUtils.doReadDate(reader.in());
            }

            case GridBinaryMarshaller.TIME: {
                if (protoCtx.client()) {
                    return convertWithTimeZone(BinaryUtils.doReadTime(reader.in()),
                        protoCtx.serverTimeZone(), TimeZone.getDefault());
                }
                else
                    return BinaryUtils.doReadTime(reader.in());
            }

            case GridBinaryMarshaller.TIMESTAMP: {
                if (protoCtx.client()) {
                    return convertWithTimeZone(BinaryUtils.doReadTimestamp(reader.in()),
                        protoCtx.serverTimeZone(), TimeZone.getDefault());
                }
                else
                    return BinaryUtils.doReadTimestamp(reader.in());
            }

            case GridBinaryMarshaller.BOOLEAN_ARR:
                return BinaryUtils.doReadBooleanArray(reader.in());

            case GridBinaryMarshaller.BYTE_ARR:
                return BinaryUtils.doReadByteArray(reader.in());

            case GridBinaryMarshaller.CHAR_ARR:
                return BinaryUtils.doReadCharArray(reader.in());

            case GridBinaryMarshaller.SHORT_ARR:
                return BinaryUtils.doReadShortArray(reader.in());

            case GridBinaryMarshaller.INT_ARR:
                return BinaryUtils.doReadIntArray(reader.in());

            case GridBinaryMarshaller.LONG_ARR:
                return BinaryUtils.doReadLongArray(reader.in());

            case GridBinaryMarshaller.FLOAT_ARR:
                return BinaryUtils.doReadFloatArray(reader.in());

            case GridBinaryMarshaller.DOUBLE_ARR:
                return BinaryUtils.doReadDoubleArray(reader.in());

            case GridBinaryMarshaller.STRING_ARR:
                return BinaryUtils.doReadStringArray(reader.in());

            case GridBinaryMarshaller.DECIMAL_ARR:
                return BinaryUtils.doReadDecimalArray(reader.in());

            case GridBinaryMarshaller.UUID_ARR:
                return BinaryUtils.doReadUuidArray(reader.in());

            case GridBinaryMarshaller.TIME_ARR:
                return BinaryUtils.doReadTimeArray(reader.in());

            case GridBinaryMarshaller.TIMESTAMP_ARR:
                return BinaryUtils.doReadTimestampArray(reader.in());

            case GridBinaryMarshaller.DATE_ARR:
                return BinaryUtils.doReadDateArray(reader.in());

            default:
                reader.in().position(reader.in().position() - 1);

                if (binObjAllow)
                    return reader.readObjectDetached();
                else
                    throw new BinaryObjectException("Custom objects are not supported");
        }
    }

    /**
     * @param writer Writer.
     * @param obj Object to write.
     * @param binObjAllow Allow to write non plain objects.
     * @param protoCtx Protocol context.
     * @throws BinaryObjectException On error.
     */
    public static void writeObject(BinaryWriterExImpl writer, @Nullable Object obj, boolean binObjAllow,
        JdbcProtocolContext protoCtx)
        throws BinaryObjectException {
        if (obj == null) {
            writer.writeByte(GridBinaryMarshaller.NULL);

            return;
        }

        Class<?> cls = obj.getClass();

        if (cls == Boolean.class)
            writer.writeBooleanFieldPrimitive((Boolean)obj);
        else if (cls == Byte.class)
            writer.writeByteFieldPrimitive((Byte)obj);
        else if (cls == Character.class)
            writer.writeCharFieldPrimitive((Character)obj);
        else if (cls == Short.class)
            writer.writeShortFieldPrimitive((Short)obj);
        else if (cls == Integer.class)
            writer.writeIntFieldPrimitive((Integer)obj);
        else if (cls == Long.class)
            writer.writeLongFieldPrimitive((Long)obj);
        else if (cls == Float.class)
            writer.writeFloatFieldPrimitive((Float)obj);
        else if (cls == Double.class)
            writer.writeDoubleFieldPrimitive((Double)obj);
        else if (cls == String.class)
            writer.doWriteString((String)obj);
        else if (cls == BigDecimal.class)
            writer.doWriteDecimal((BigDecimal)obj);
        else if (cls == UUID.class)
            writer.writeUuid((UUID)obj);
        else if (cls == java.sql.Date.class || cls == java.util.Date.class) {
            if (protoCtx.client()) {
                writer.writeDate(
                    convertWithTimeZone((java.util.Date)obj, TimeZone.getDefault(), protoCtx.serverTimeZone()));
            }
            else
                writer.writeDate((java.util.Date)obj);
        }
        else if (cls == Time.class) {
            if (protoCtx.client()) {
                writer.writeTime(
                    convertWithTimeZone((Time)obj, TimeZone.getDefault(), protoCtx.serverTimeZone()));
            }
            else
                writer.writeTime((Time)obj);
        }
        else if (cls == Timestamp.class) {
            if (protoCtx.client()) {
                writer.writeTimestamp(
                    convertWithTimeZone((Timestamp)obj, TimeZone.getDefault(), protoCtx.serverTimeZone()));
            } else
                writer.writeTimestamp((Timestamp)obj);
        }
        else if (cls == boolean[].class)
            writer.writeBooleanArray((boolean[])obj);
        else if (cls == byte[].class)
            writer.writeByteArray((byte[])obj);
        else if (cls == char[].class)
            writer.writeCharArray((char[])obj);
        else if (cls == short[].class)
            writer.writeShortArray((short[])obj);
        else if (cls == int[].class)
            writer.writeIntArray((int[])obj);
        else if (cls == long[].class)
            writer.writeLongArray((long[])obj);
        else if (cls == float[].class)
            writer.writeFloatArray((float[])obj);
        else if (cls == double[].class)
            writer.writeDoubleArray((double[])obj);
        else if (cls == String[].class)
            writer.writeStringArray((String[])obj);
        else if (cls == BigDecimal[].class)
            writer.writeDecimalArray((BigDecimal[])obj);
        else if (cls == UUID[].class)
            writer.writeUuidArray((UUID[])obj);
        else if (cls == Time[].class)
            writer.writeTimeArray((Time[])obj);
        else if (cls == Timestamp[].class)
            writer.writeTimestampArray((Timestamp[])obj);
        else if (cls == java.util.Date[].class || cls == java.sql.Date[].class)
            writer.writeDateArray((java.util.Date[])obj);
        else if (binObjAllow)
            writer.writeObjectDetached(obj);
        else
            throw new BinaryObjectException("Custom objects are not supported");
    }

    /**
     */
    public static Timestamp convertWithTimeZone(Timestamp ts, TimeZone tzFrom, TimeZone tzTo) {
        if (tzTo == null || tzFrom == null)
            return ts;

        Instant i = Instant.ofEpochMilli(ts.getTime());

        LocalDateTime ldt = LocalDateTime.ofInstant(i, tzFrom.toZoneId());

        ZonedDateTime zdt = ldt.atZone(tzTo.toZoneId());

        return new Timestamp(zdt.toInstant().toEpochMilli());
    }

    /**
     */
    public static Time convertWithTimeZone(Time t, TimeZone tzFrom, TimeZone tzTo) {
        if (tzTo == null || tzFrom == null)
            return t;

        Instant i = Instant.ofEpochMilli(t.getTime());

        LocalDateTime ldt = LocalDateTime.ofInstant(i, tzFrom.toZoneId());

        ZonedDateTime zdt = ldt.atZone(tzTo.toZoneId());

        return new Time(zdt.toInstant().toEpochMilli());
    }

    /**
     */
    public static java.util.Date convertWithTimeZone(java.util.Date t, TimeZone tzFrom, TimeZone tzTo) {
        if (tzTo == null || tzFrom == null)
            return t;

        Instant i = Instant.ofEpochMilli(t.getTime());

        LocalDateTime ldt = LocalDateTime.ofInstant(i, tzFrom.toZoneId());

        ZonedDateTime zdt = ldt.atZone(tzTo.toZoneId());

        return new Time(zdt.toInstant().toEpochMilli());
    }
}

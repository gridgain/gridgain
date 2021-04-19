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

import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.odbc.SqlListenerUtils;
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
                    writeObject(writer, obj, protoCtx);
            }
        }
    }

    /**
     * @param reader Binary reader.
     * @param protoCtx Protocol context.
     * @return Query results items.
     */
    public static List<List<Object>> readItems(BinaryReaderExImpl reader, JdbcProtocolContext protoCtx) {
        int rowsSize = reader.readInt();

        if (rowsSize > 0) {
            List<List<Object>> items = new ArrayList<>(rowsSize);

            for (int i = 0; i < rowsSize; ++i) {
                int colsSize = reader.readInt();

                List<Object> col = new ArrayList<>(colsSize);

                for (int colCnt = 0; colCnt < colsSize; ++colCnt)
                    col.add(readObject(reader, protoCtx));

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
     * @param protoCtx Protocol context.
     * @return Read object.
     * @throws BinaryObjectException On error.
     */
    @Nullable public static Object readObject(BinaryReaderExImpl reader, JdbcProtocolContext protoCtx)
        throws BinaryObjectException {
        byte type = reader.readByte();

        switch (type) {
            case GridBinaryMarshaller.DATE: {
                if (protoCtx.client()) {
                    return new java.sql.Date(SqlListenerUtils.convertWithTimeZone(BinaryUtils.doReadDate(reader.in()),
                        protoCtx.serverTimeZone(), TimeZone.getDefault()).getTime());
                }
                else
                    return BinaryUtils.doReadDate(reader.in());
            }

            case GridBinaryMarshaller.TIME: {
                if (protoCtx.client()) {
                    return SqlListenerUtils.convertWithTimeZone(BinaryUtils.doReadTime(reader.in()),
                        protoCtx.serverTimeZone(), TimeZone.getDefault());
                }
                else
                    return BinaryUtils.doReadTime(reader.in());
            }

            case GridBinaryMarshaller.TIMESTAMP: {
                if (protoCtx.client()) {
                    return SqlListenerUtils.convertWithTimeZone(BinaryUtils.doReadTimestamp(reader.in()),
                        protoCtx.serverTimeZone(), TimeZone.getDefault());
                }
                else
                    return BinaryUtils.doReadTimestamp(reader.in());
            }

            default:
                return SqlListenerUtils.readObject(type, reader,
                    protoCtx.isFeatureSupported(JdbcThinFeature.CUSTOM_OBJECT), protoCtx.keepBinary());
        }
    }

    /**
     * @param writer Writer.
     * @param obj Object to write.
     * @param protoCtx Protocol context.
     * @throws BinaryObjectException On error.
     */
    public static void writeObject(BinaryWriterExImpl writer, @Nullable Object obj, JdbcProtocolContext protoCtx)
        throws BinaryObjectException {
        if (obj == null) {
            writer.writeByte(GridBinaryMarshaller.NULL);

            return;
        }

        Class<?> cls = obj.getClass();

        if (cls == java.sql.Date.class || cls == java.util.Date.class) {
            if (protoCtx.client()) {
                writer.writeDate(
                        SqlListenerUtils.convertWithTimeZone((java.util.Date)obj, TimeZone.getDefault(),
                                protoCtx.serverTimeZone()));
            }
            else
                writer.writeDate((java.util.Date)obj);
        }
        else if (cls == Time.class) {
            if (protoCtx.client()) {
                writer.writeTime(
                        SqlListenerUtils.convertWithTimeZone((Time)obj, TimeZone.getDefault(),
                                protoCtx.serverTimeZone()));
            }
            else
                writer.writeTime((Time)obj);
        }
        else if (cls == Timestamp.class) {
            if (protoCtx.client()) {
                writer.writeTimestamp(
                    SqlListenerUtils.convertWithTimeZone((Timestamp)obj, TimeZone.getDefault(),
                            protoCtx.serverTimeZone()));
            } else
                writer.writeTimestamp((Timestamp)obj);
        }
        else
            SqlListenerUtils.writeObject(writer, obj, protoCtx.isFeatureSupported(JdbcThinFeature.CUSTOM_OBJECT));
    }
}

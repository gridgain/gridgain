/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.odbc.SqlListenerUtils;
import org.jetbrains.annotations.Nullable;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;

/**
 * SQL-related utility methods for thin client.
 */
public class ClientSqlUtils {
    /**
     * Read arguments for SQL query.
     *
     * @param reader Reader.
     * @param protoCtx Protocol context.
     * @return Arguments.
     */
    @Nullable public static Object[] readQueryArgs(BinaryRawReaderEx reader, ClientProtocolContext protoCtx) {
        int cnt = reader.readInt();

        if (cnt > 0) {
            Object[] args = new Object[cnt];

            for (int i = 0; i < cnt; i++)
                args[i] = readSqlField(reader, protoCtx);

            return args;
        }
        else
            return null;
    }

    /**
     * @param reader Reader.
     * @param protoCtx Protocol context.
     * @return Read object.
     * @throws BinaryObjectException On error.
     */
    @Nullable
    public static Object readSqlField(BinaryRawReaderEx reader, ClientProtocolContext protoCtx)
            throws BinaryObjectException {
        BinaryReaderExImpl reader0 = (BinaryReaderExImpl) reader;
        TimeZone serverTz = TimeZone.getDefault();
        TimeZone clientTz = protoCtx.clientTimeZone();

        byte type = reader0.readByte();
        switch (type) {
            case GridBinaryMarshaller.DATE: {
                return new java.sql.Date(SqlListenerUtils.convertWithTimeZone(
                        BinaryUtils.doReadDate(reader0.in()),clientTz, serverTz).getTime());
            }

            case GridBinaryMarshaller.TIME: {
                return SqlListenerUtils.convertWithTimeZone(
                        BinaryUtils.doReadTime(reader0.in()), clientTz, serverTz);
            }

            case GridBinaryMarshaller.TIMESTAMP: {
                return SqlListenerUtils.convertWithTimeZone(
                        BinaryUtils.doReadTimestamp(reader0.in()), clientTz, serverTz);
            }

            default:
                reader0.in().position(reader0.in().position() - 1);
                return reader0.readObjectDetached();
        }
    }

    /**
     * @param writer Writer.
     * @param obj Object to write.
     * @throws BinaryObjectException On error.
     */
    public static void writeSqlField(BinaryRawWriterEx writer, @Nullable Object obj, ClientProtocolContext protoCtx)
            throws BinaryObjectException {
        Class<?> cls = obj == null ? null : obj.getClass();

        TimeZone serverTz = TimeZone.getDefault();
        TimeZone clientTz = protoCtx.clientTimeZone();

        if (cls == java.sql.Date.class || cls == java.util.Date.class) {
            writer.writeDate(SqlListenerUtils.convertWithTimeZone((java.util.Date)obj, serverTz, clientTz));
        }
        else if (cls == Time.class) {
            writer.writeTime(SqlListenerUtils.convertWithTimeZone((Time)obj, serverTz, clientTz));
        }
        else if (cls == Timestamp.class) {
            writer.writeTimestamp(SqlListenerUtils.convertWithTimeZone((Timestamp)obj, serverTz, clientTz));
        }
        else
            writer.writeObjectDetached(obj);
    }
}

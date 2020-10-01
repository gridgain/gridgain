/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage;

import java.util.Arrays;
import org.junit.Test;

import static org.apache.ignite.internal.storage.NativeType.INTEGER;
import static org.apache.ignite.internal.storage.NativeType.LONG;
import static org.apache.ignite.internal.storage.NativeType.STRING;
import static org.apache.ignite.internal.storage.NativeType.VARLONG;
import static org.junit.Assert.assertEquals;

/**
 * Tests tuple assembling and reading.
 * TODO generate schemas
 * TODO more UTF-8 sequences
 * TODO test sizing methods and move to tuple assembler
 */
public class TupleTest {
    public static final long[] VARLONG_BOUNDARIES = {
        0xFFFFFFFFFFFFFFFFL >>> (64 - 7),
        0xFFFFFFFFFFFFFFFFL >>> (64 - 14),
        0xFFFFFFFFFFFFFFFFL >>> (64 - 21),
        0xFFFFFFFFFFFFFFFFL >>> (64 - 28),
        0xFFFFFFFFFFFFFFFFL >>> (64 - 35),
        0xFFFFFFFFFFFFFFFFL >>> (64 - 42),
        0xFFFFFFFFFFFFFFFFL >>> (64 - 49),
        0xFFFFFFFFFFFFFFFFL >>> (64 - 56),
        0xFFFFFFFFFFFFFFFFL >>> (64 - 63)
    };

    @Test
    public void testFixedSizes() {
        checkSchema(
            new NativeType[]{INTEGER},
            new NativeType[]{LONG});

        checkSchema(
            new NativeType[]{INTEGER, LONG},
            new NativeType[]{INTEGER, INTEGER, LONG, LONG});

        checkSchema(
            new NativeType[]{INTEGER, INTEGER, LONG, LONG},
            new NativeType[]{INTEGER, INTEGER, LONG, LONG});

        checkSchema(
            new NativeType[]{INTEGER, INTEGER, INTEGER, LONG, LONG, LONG},
            new NativeType[]{INTEGER, INTEGER, LONG, LONG, LONG, LONG});

        checkSchema(
            new NativeType[]{INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, LONG, LONG, LONG},
            new NativeType[]{INTEGER, INTEGER, LONG, LONG, LONG, LONG});

        checkSchema(
            new NativeType[]{INTEGER, LONG},
            new NativeType[]{INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG});

        checkSchema(
            new NativeType[]{INTEGER, INTEGER, LONG, LONG},
            new NativeType[]{INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG});
    }

    @Test
    public void testVariableSizes() {
        checkSchema(
            new NativeType[]{VARLONG},
            new NativeType[]{VARLONG});

        checkSchema(
            new NativeType[]{STRING},
            new NativeType[]{STRING});

        checkSchema(
            new NativeType[]{VARLONG, VARLONG},
            new NativeType[]{VARLONG, VARLONG});

        checkSchema(
            new NativeType[]{STRING, VARLONG},
            new NativeType[]{STRING, VARLONG});

        checkSchema(
            new NativeType[]{STRING, STRING},
            new NativeType[]{STRING, STRING});

        checkSchema(
            new NativeType[]{VARLONG, VARLONG, VARLONG, VARLONG, VARLONG},
            new NativeType[]{VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG});

        checkSchema(
            new NativeType[]{VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG},
            new NativeType[]{VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG});

        checkSchema(
            new NativeType[]{STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, VARLONG, VARLONG},
            new NativeType[]{STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, VARLONG, VARLONG, VARLONG});
    }

    @Test
    public void testMixedSizes() {
        checkSchema(
            new NativeType[]{INTEGER, VARLONG},
            new NativeType[]{LONG, VARLONG});

        checkSchema(
            new NativeType[]{INTEGER, STRING},
            new NativeType[]{LONG, STRING});

        checkSchema(
            new NativeType[]{INTEGER, INTEGER, LONG, LONG, LONG, LONG, LONG, LONG, VARLONG, VARLONG, VARLONG, VARLONG},
            new NativeType[]{INTEGER, INTEGER, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, VARLONG, VARLONG, VARLONG});

        checkSchema(
            new NativeType[]{INTEGER, INTEGER, LONG, LONG, LONG, LONG, LONG, LONG, STRING, STRING, VARLONG, VARLONG},
            new NativeType[]{INTEGER, INTEGER, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG, STRING, STRING, VARLONG});
    }

    private void checkSchema(NativeType[] keys, NativeType[] vals) {
        SchemaDescriptor sch = schema(keys, vals, true);

        Object[] checkArr = sequence(sch);

        checkValues(schema(keys, vals, false), checkArr);

        checkValues(sch, checkArr);

        while (checkArr[0] != null) {
            int idx = 0;

            Object prev = checkArr[idx];
            checkArr[idx] = null;

            checkValues(sch, checkArr);

            while (idx < checkArr.length - 1 && checkArr[idx + 1] != null) {
                checkArr[idx] = prev;
                prev = checkArr[idx + 1];
                checkArr[idx + 1] = null;
                idx++;

                checkValues(sch, checkArr);
            }
        }
    }

    private Object[] sequence(SchemaDescriptor schema) {
        Object[] res = new Object[schema.length()];

        for (int i = 0; i < res.length; i++) {
            NativeType type = schema.column(i).type();

            if (type == INTEGER || type == LONG || type == VARLONG)
                res[i] = (long)((i + 1) * 50);
            else if (type == STRING)
                res[i] = "string-" + (i * 50);
        }

        return res;
    }

    private SchemaDescriptor schema(NativeType[] keys, NativeType[] vals, boolean nullable) {
        Column[] keyCols = new Column[keys.length];
        for (int i = 0; i < keyCols.length; i++)
            keyCols[i] = new Column("id" + i, keys[i], nullable);

        Column[] valCols = new Column[vals.length];
        for (int i = 0; i < valCols.length; i++)
            valCols[i] = new Column("id" + i, vals[i], nullable);

        return new SchemaDescriptor(1, new Columns(keyCols), new Columns(valCols));
    }

    private void checkValues(SchemaDescriptor schema, Object... vals) {
        System.out.println("Checking array: " + Arrays.toString(vals));

        assertEquals(schema.keyColumns().length() + schema.valueColumns().length(), vals.length);

        int nonNullVarsizeKeyCols = 0;
        int nonNullVarsizeValCols = 0;
        int nonNullVarsizeKeySize = 0;
        int nonNullVarsizeValSize = 0;

        for (int i = 0; i < vals.length; i++) {
            NativeType type = schema.column(i).type();

            if (vals[i] != null && !type.fixedSize()) {
                if (type == VARLONG) {
                    if (schema.keyColumn(i)) {
                        nonNullVarsizeKeyCols++;
                        nonNullVarsizeKeySize += varlongSize((Long)vals[i]);
                    }
                    else {
                        nonNullVarsizeValCols++;
                        nonNullVarsizeValSize += varlongSize((Long)vals[i]);
                    }
                }
                else if (type == STRING) {
                    if (schema.keyColumn(i)) {
                        nonNullVarsizeKeyCols++;
                        nonNullVarsizeKeySize += utf8EncodedLength((CharSequence)vals[i]);
                    }
                    else {
                        nonNullVarsizeValCols++;
                        nonNullVarsizeValSize += utf8EncodedLength((CharSequence)vals[i]);
                    }
                }
                else
                    throw new IllegalStateException("Unsupported test varsize type: " + type);
            }
        }

        int size = Tuple.SCHEMA_VERSION_FIELD_SIZE + Tuple.KEY_HASH_FIELD_SIZE +
            tupleChunkSize(schema.keyColumns(), nonNullVarsizeKeyCols, nonNullVarsizeKeySize) +
            tupleChunkSize(schema.valueColumns(), nonNullVarsizeValCols, nonNullVarsizeValSize);

        TupleAssembler asm = new TupleAssembler(schema, size, nonNullVarsizeKeyCols, nonNullVarsizeValCols);

        for (int i = 0; i < vals.length; i++) {
            if (vals[i] == null)
                asm.appendNull();
            else {
                NativeType type = schema.column(i).type();

                if (type == INTEGER)
                    asm.appendInt(((Long)vals[i]).intValue());
                else if (type == LONG)
                    asm.appendLong((Long)vals[i]);
                else if (type == VARLONG)
                    asm.appendVarlong((Long)vals[i]);
                else if (type == STRING)
                    asm.appendString((String)vals[i]);
                else
                    throw new IllegalStateException("Unsupported test type: " + type);
            }
        }

        byte[] data = asm.build();

        HeapTuple tup = new HeapTuple(data);

        for (int i = 0; i < vals.length; i++) {
            NativeType type = schema.column(i).type();
            Object res;

            if (type == INTEGER)
                res = (long)tup.intValue(schema, i);
            else if (type == LONG)
                res = tup.longValue(schema, i);
            else if (type == VARLONG)
                res = tup.varlongValue(schema, i);
            else if (type == STRING)
                res = tup.stringValue(schema, i);
            else
                throw new IllegalStateException("Unsupported test type: " + type);

            if (type == STRING)
                assertEquals(vals[i], res);
            else
                assertEquals(vals[i] == null ? 0L : vals[i], res);
        }
    }

    private int tupleChunkSize(Columns cols, int nonNullVarsizeCols, int nonNullVarsizeSize) {
        int size = Tuple.TOTAL_LEN_FIELD_SIZE + Tuple.VARSIZE_TABLE_LEN_FIELD_SIZE +
            TupleAssembler.varsizeTableSize(nonNullVarsizeCols) + cols.nullMapSize();

        for (int i = 0; i < cols.numberOfFixsizeColumns(); i++)
            size += cols.column(i).type().size();

        return size + nonNullVarsizeSize;
    }

    private static int varlongSize(long val) {
        if (val < 0)
            return 10;

        int idx = 0;

        while (val > VARLONG_BOUNDARIES[idx])
            idx++;

        return idx + 1;
    }

    /**
     * This implementation is not tolerant to malformed char sequences.
     */
    public static int utf8EncodedLength(CharSequence sequence) {
        int cnt = 0;

        for (int i = 0, len = sequence.length(); i < len; i++) {
            char ch = sequence.charAt(i);

            if (ch <= 0x7F)
                cnt++;
            else if (ch <= 0x7FF)
                cnt += 2;
            else if (Character.isHighSurrogate(ch)) {
                cnt += 4;
                ++i;
            }
            else
                cnt += 3;
        }

        return cnt;
    }
}

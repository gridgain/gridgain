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

package org.apache.ignite.internal.storage.testing;

import java.util.Arrays;
import org.apache.ignite.internal.storage.Column;
import org.apache.ignite.internal.storage.Columns;
import org.apache.ignite.internal.storage.NativeType;
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
public class ValueTupleTest {
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
            new NativeType[]{INTEGER});

        checkSchema(
            new NativeType[]{LONG});

        checkSchema(
            new NativeType[]{INTEGER, INTEGER, LONG, LONG});

        checkSchema(
            new NativeType[]{INTEGER, INTEGER, LONG, LONG});

        checkSchema(
            new NativeType[]{INTEGER, INTEGER, INTEGER, LONG, LONG, LONG});

        checkSchema(
            new NativeType[]{INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, LONG, LONG, LONG});

        checkSchema(
            new NativeType[]{INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG});

        checkSchema(
            new NativeType[]{INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, LONG, LONG, LONG, LONG, LONG, LONG, LONG, LONG});
    }

    @Test
    public void testVariableSizes() {
        checkSchema(
            new NativeType[]{VARLONG});

        checkSchema(
            new NativeType[]{STRING});

        checkSchema(
            new NativeType[]{VARLONG, VARLONG});

        checkSchema(
            new NativeType[]{STRING, VARLONG});

        checkSchema(
            new NativeType[]{STRING, STRING});

        checkSchema(
            new NativeType[]{VARLONG, VARLONG, VARLONG, VARLONG, VARLONG});

        checkSchema(
            new NativeType[]{STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, VARLONG, VARLONG});

        checkSchema(
            new NativeType[]{VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG, VARLONG});
    }

    @Test
    public void testMixedSizes() {
        checkSchema(
            new NativeType[]{INTEGER, VARLONG});

        checkSchema(
            new NativeType[]{INTEGER, STRING});

        checkSchema(
            new NativeType[]{INTEGER, INTEGER, LONG, LONG, LONG, LONG, LONG, LONG, STRING, STRING, VARLONG, VARLONG});

        checkSchema(
            new NativeType[]{INTEGER, INTEGER, LONG, LONG, LONG, LONG, LONG, LONG, VARLONG, VARLONG, VARLONG, VARLONG});
    }

    private void checkSchema(NativeType[] types) {
        Columns cols = schema(types, true);

        Object[] checkArr = sequence(cols);

        checkValues(schema(types, false), checkArr);

        checkValues(cols, checkArr);

        while (checkArr[0] != null) {
            int idx = 0;

            Object prev = checkArr[idx];
            checkArr[idx] = null;

            checkValues(cols, checkArr);

            while (idx < checkArr.length - 1 && checkArr[idx + 1] != null) {
                checkArr[idx] = prev;
                prev = checkArr[idx + 1];
                checkArr[idx + 1] = null;
                idx++;

                checkValues(cols, checkArr);
            }
        }
    }

    private Object[] sequence(Columns cols) {
        Object[] res = new Object[cols.length()];

        for (int i = 0; i < res.length; i++) {
            NativeType type = cols.column(i).type();

            if (type == INTEGER || type == LONG || type == VARLONG)
                res[i] = (long)((i + 1) * 50);
            else if (type == STRING)
                res[i] = "string-" + (i * 50);
        }

        return res;
    }

    private Columns schema(NativeType[] types, boolean nullable) {
        Column[] cols = new Column[types.length];
        for (int i = 0; i < cols.length; i++)
            cols[i] = new Column("id" + i, types[i], nullable);

        return new Columns(cols);
    }

    private void checkValues(Columns cols, Object... vals) {
        System.out.println("Checking array: " + Arrays.toString(vals));

        assertEquals(cols.length(), vals.length);

        int nonNullVarsizeCols = 0;
        int nonNullVarsizeSize = 0;

        for (int i = 0; i < vals.length; i++) {
            NativeType type = cols.column(i).type();

            if (vals[i] != null && !type.fixedSize()) {
                if (type == VARLONG) {
                    nonNullVarsizeCols++;
                    nonNullVarsizeSize += varlongSize((Long)vals[i]);
                }
                else if (type == STRING) {
                    nonNullVarsizeCols++;
                    nonNullVarsizeSize += utf8EncodedLength((CharSequence)vals[i]);
                }
                else
                    throw new IllegalStateException("Unsupported test varsize type: " + type);
            }
        }

        int size = tupleChunkSize(cols, nonNullVarsizeCols, nonNullVarsizeSize);

        ValueTupleAssembler asm = new ValueTupleAssembler(cols, size, nonNullVarsizeCols);

        for (int i = 0; i < vals.length; i++) {
            if (vals[i] == null)
                asm.appendNull();
            else {
                NativeType type = cols.column(i).type();

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

        HeapValueTuple tup = new HeapValueTuple(data);

        for (int i = 0; i < vals.length; i++) {
            NativeType type = cols.column(i).type();
            Object res;

            if (type == INTEGER)
                res = (long)tup.intValue(cols, i);
            else if (type == LONG)
                res = tup.longValue(cols, i);
            else if (type == VARLONG)
                res = tup.varlongValue(cols, i);
            else if (type == STRING)
                res = tup.stringValue(cols, i);
            else
                throw new IllegalStateException("Unsupported test type: " + type);

            if (type == STRING)
                assertEquals(vals[i], res);
            else
                assertEquals(vals[i] == null ? 0L : vals[i], res);
        }
    }

    private int tupleChunkSize(Columns cols, int nonNullVarsizeCols, int nonNullVarsizeSize) {
        int size = ValueTuple.TOTAL_LEN_FIELD_SIZE + ValueTuple.VARSIZE_TABLE_LEN_FIELD_SIZE +
            ValueTupleAssembler.varsizeTableSize(nonNullVarsizeCols) + cols.nullMapSize();

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

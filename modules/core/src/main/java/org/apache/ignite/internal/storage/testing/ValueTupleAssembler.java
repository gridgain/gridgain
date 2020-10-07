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

import java.math.BigDecimal;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.binary.BinaryPrimitives;
import org.apache.ignite.internal.storage.Column;
import org.apache.ignite.internal.storage.Columns;
import org.apache.ignite.internal.storage.NativeType;
import org.apache.ignite.internal.storage.Tuple;
import org.apache.ignite.internal.storage.TupleAssembler;

/**
 *
 */
public class ValueTupleAssembler {
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

    public static int varlongSize(long val) {
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

    public static int varsizeTableSize(int nonNullVarsizeCols) {
        return nonNullVarsizeCols * 2;
    }

    public static int tupleChunkSize(Columns cols, int nonNullVarsizeCols, int nonNullVarsizeSize) {
        int size = Tuple.TOTAL_LEN_FIELD_SIZE + Tuple.VARSIZE_TABLE_LEN_FIELD_SIZE +
            TupleAssembler.varsizeTableSize(nonNullVarsizeCols) + cols.nullMapSize();

        for (int i = 0; i < cols.numberOfFixsizeColumns(); i++)
            size += cols.column(i).type().size();

        return size + nonNullVarsizeSize;
    }

    /** */
    private final Columns cols;

    private final byte[] arr;

    /** Current field index (the field is unset). */
    private int curCol;

    /** */
    private int curVarsizeTblEntry;

    private int curOff;

    private int nullMapOff;

    private int varsizeTblOff;

    private CharsetEncoder stringEncoder;

    public ValueTupleAssembler(
        Columns cols,
        int size,
        int nonNullVarsizeCols
    ) {
        this.cols = cols;

        arr = new byte[size];

        initOffsets(nonNullVarsizeCols);
    }

    public static int bigDecimalSize(BigDecimal val) {
        return 4 + val.unscaledValue().bitLength() / 8 + 1;
    }

    public void appendNull() {
        Column col = cols.column(curCol);

        if (!col.nullable())
            throw new IllegalArgumentException("Failed to set column (null was passed, but column is not nullable): " +
                col);

        setNull(curCol);

        shiftColumn(0, false);
    }

    public void appendInt(int val) {
        checkType(NativeType.INTEGER);

        BinaryPrimitives.writeInt(arr, curOff, val);

        shiftColumn(NativeType.INTEGER.size(), false);
    }

    public void appendShort(short val) {
        checkType(NativeType.SHORT);

        BinaryPrimitives.writeShort(arr, curOff, val);

        shiftColumn(NativeType.SHORT.size(), false);
    }

    public void appendLong(long val) {
        checkType(NativeType.LONG);

        BinaryPrimitives.writeLong(arr, curOff, val);

        shiftColumn(NativeType.LONG.size(), false);
    }

    public void appendVarlong(long val) {
        checkType(NativeType.VARLONG);

        int size = BinaryPrimitives.writeVarlong(arr, curOff, val);
        writeOffset(curVarsizeTblEntry, curOff);

        assert size == varlongSize(val);

        shiftColumn(size, true);
    }

    public void appendString(String val) {
        checkType(NativeType.STRING);

        ByteBuffer wrapper = ByteBuffer.wrap(arr, curOff, arr.length - curOff);

        CharsetEncoder encoder = encoder();
        encoder.reset();
        CoderResult cr = encoder.encode(CharBuffer.wrap(val), wrapper, true);

        if (!cr.isUnderflow())
            throw new BufferUnderflowException();

        cr = encoder.flush(wrapper);

        if (!cr.isUnderflow())
            throw new BufferUnderflowException();

        writeOffset(curVarsizeTblEntry, curOff);

        assert wrapper.position() - curOff == utf8EncodedLength(val);

        shiftColumn(wrapper.position() - curOff, true);
    }

    public void appendBigDecimal(BigDecimal val) {
        checkType(NativeType.BIGDECIMAL);

        byte[] intBytes = val.unscaledValue().toByteArray();

        BinaryPrimitives.writeInt(arr, curOff, val.scale());

        System.arraycopy(intBytes, 0, arr, curOff + 4, intBytes.length);

        writeOffset(curVarsizeTblEntry, curOff);

        assert intBytes.length + 4 == bigDecimalSize(val) : "Failed [bl=" + intBytes.length +
            ", bds=" + bigDecimalSize(val) + ", bd=" + val;

        shiftColumn(intBytes.length + 4, true);
    }

    private CharsetEncoder encoder() {
        if (stringEncoder == null)
            stringEncoder = StandardCharsets.UTF_8.newEncoder();

        return stringEncoder;
    }

    public byte[] build() {
        return arr;
    }

    private void writeOffset(int tblEntryIdx, int writtenOff) {
        BinaryPrimitives.writeShort(arr, varsizeTblOff + 2 * tblEntryIdx, (short)writtenOff);
    }

    private void checkType(NativeType type) {
        Column col = cols.column(curCol);

        if (col.type() != type)
            throw new IllegalArgumentException("Failed to set column (int was passed, but column is of different " +
                "type): " + col);
    }

    private void setNull(int curCol) {
        int byteInMap = curCol / 8;
        int bitInByte = curCol % 8;

        arr[nullMapOff + byteInMap] |= 1 << bitInByte;
    }

    private void shiftColumn(int size, boolean varsize) {
        curCol++;
        curOff += size;

        if (varsize && size != 0)
            curVarsizeTblEntry++;

        if (curCol == cols.length()) {
            int keyLen = curOff;

            BinaryPrimitives.writeShort(arr, 0, (short)keyLen);

// TODO                seal();
        }
    }

    private void initOffsets(int nonNullVarsizeCols) {
        curCol = 0;
        curVarsizeTblEntry = 0;

        BinaryPrimitives.writeShort(arr, ValueTuple.TOTAL_LEN_FIELD_SIZE, (short)nonNullVarsizeCols);

        varsizeTblOff = ValueTuple.TOTAL_LEN_FIELD_SIZE + ValueTuple.VARSIZE_TABLE_LEN_FIELD_SIZE;
        nullMapOff = varsizeTblOff + varsizeTableSize(nonNullVarsizeCols);
        curOff = nullMapOff + cols.nullMapSize();
    }
}

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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.binary.BinaryPrimitives;

/**
 *
 */
public class TupleAssembler {
    /** */
    private final SchemaDescriptor schema;

    private final int nonNullVarsizeValCols;

    private final byte[] arr;

    /** */
    private Columns curCols;

    /** Current field index (the field is unset). */
    private int curCol;

    /** */
    private int curVarsizeTblEntry;

    private int baseOff;

    private int curOff;

    private int nullMapOff;

    private int varsizeTblOff;

    private CharsetEncoder stringEncoder;

    /** */
    private int keyHash;

    public static int varsizeTableSize(int nonNullVarsizeCols) {
        return nonNullVarsizeCols * 2;
    }

    public TupleAssembler(
        SchemaDescriptor schema,
        int size,
        int nonNullVarsizeKeyCols,
        int nonNullVarsizeValCols
    ) {
        this.schema = schema;

        this.nonNullVarsizeValCols = nonNullVarsizeValCols;

        arr = new byte[size];

        curCols = schema.columns(0);

        initOffsets(Tuple.SCHEMA_VERSION_FIELD_SIZE + Tuple.KEY_HASH_FIELD_SIZE, nonNullVarsizeKeyCols);

        BinaryPrimitives.writeShort(arr, 0, (short)schema.version());
    }

    public void appendNull() {
        Column col = curCols.column(curCol);

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

    public void appendLong(long val) {
        checkType(NativeType.LONG);

        BinaryPrimitives.writeLong(arr, curOff, val);

        shiftColumn(NativeType.LONG.size(), false);
    }

    public void appendVarlong(long val) {
        checkType(NativeType.VARLONG);

        int size = BinaryPrimitives.writeVarlong(arr, curOff, val);
        writeOffset(curVarsizeTblEntry, curOff - baseOff);

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

        writeOffset(curVarsizeTblEntry, curOff - baseOff);

        shiftColumn(wrapper.position() - curOff, true);
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

    private void checkType(NativeType integer) {
        Column col = curCols.column(curCol);

        if (col.type() != integer)
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

        if (curCol == curCols.length()) {
            Columns cols = schema.columns(curCol);

            int keyLen = curOff - baseOff;

            BinaryPrimitives.writeShort(arr, baseOff, (short)keyLen);

            if (cols == curCols) {
// TODO                seal();

                return;
            }

            curCols = cols;

            initOffsets(baseOff + keyLen, nonNullVarsizeValCols);
        }
    }

    private void initOffsets(int base, int nonNullVarsizeCols) {
        baseOff = base;

        curCol = 0;
        curVarsizeTblEntry = 0;

        BinaryPrimitives.writeShort(arr, baseOff + Tuple.TOTAL_LEN_FIELD_SIZE, (short)nonNullVarsizeCols);

        varsizeTblOff = baseOff + Tuple.TOTAL_LEN_FIELD_SIZE + Tuple.VARSIZE_TABLE_LEN_FIELD_SIZE;
        nullMapOff = varsizeTblOff + varsizeTableSize(nonNullVarsizeCols);
        curOff = nullMapOff + curCols.nullMapSize();
    }
}

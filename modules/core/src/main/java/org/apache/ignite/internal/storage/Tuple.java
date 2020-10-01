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

/**
 * Tuple structure:
 *
 * <pre>
 * +---------+---------+-------+-------+
 * |  Schema |    Key  |  Key  | Value |
 * | Version |   Hash  | Bytes | Bytes |
 * +---------+---------+-------+-------+
 * | 2 bytes | 4 bytes |               |
 * +---------+---------+-------+-------+
 * </pre>
 * Each bytes section has the following structure:
 * <pre>
 * +-------------------------------------------------------+
 * |   Total | Vartable |  Varlen | Null | Fixlen | Varlen |
 * |  Length |   Length | Offsets |  Map |  Bytes |  Bytes |
 * +---------+---------------------------------------------+
 * | 2 bytes |  2 bytes |                                  |
 * +---------+---------------------------------------------+
 * </pre>
 * TODO varlen table is optional and can be omitted if there are no varlen columns
 * TODO nulls are not returned rn for integer types
 * TODO varlong should be written with signum
 */
public abstract class Tuple {
    public static final int SCHEMA_VERSION_FIELD_SIZE = 2;
    public static final int KEY_HASH_FIELD_SIZE = 4;
    public static final int TOTAL_LEN_FIELD_SIZE = 2;
    public static final int VARSIZE_TABLE_LEN_FIELD_SIZE = 2;

    public int intValue(SchemaDescriptor schema, int col) {
        // Get base offset (key start or value start) for the given column.
        int baseOff = baseOffset(schema, col);
        int idxFromBase = indexFromBase(schema, col);
        Columns cols = schema.columns(col);

        checkColumn(cols, idxFromBase, NativeType.INTEGER);

        if (isNull(baseOff, idxFromBase))
            return 0;

        int off = fixlenColumnOffset(cols, baseOff, idxFromBase);

        return readInteger(off);
    }

    public long longValue(SchemaDescriptor schema, int col) {
        // Get base offset (key start or value start) for the given column.
        int baseOff = baseOffset(schema, col);
        int idxFromBase = indexFromBase(schema, col);
        Columns cols = schema.columns(col);

        checkColumn(cols, idxFromBase, NativeType.LONG);

        if (isNull(baseOff, idxFromBase))
            return 0;

        int off = fixlenColumnOffset(cols, baseOff, idxFromBase);

        return readLong(off);
    }

    public long varlongValue(SchemaDescriptor schema, int col) {
        // Get base offset (key start or value start) for the given column.
        int baseOff = baseOffset(schema, col);
        int idxFromBase = indexFromBase(schema, col);
        Columns cols = schema.columns(col);

        checkColumn(cols, idxFromBase, NativeType.VARLONG);

        if (isNull(baseOff, idxFromBase))
            return 0;

        long off = varlenColumnOffsetAndLength(cols, baseOff, idxFromBase, NativeType.VARLONG.fixedSize());

        return readVarlong((int)(off));
    }

    public String stringValue(SchemaDescriptor schema, int col) {
        // Get base offset (key start or value start) for the given column.
        int baseOff = baseOffset(schema, col);
        int idxFromBase = indexFromBase(schema, col);
        Columns cols = schema.columns(col);

        checkColumn(cols, idxFromBase, NativeType.STRING);

        if (isNull(baseOff, idxFromBase))
            return null;

        long offLen = varlenColumnOffsetAndLength(cols, baseOff, idxFromBase, NativeType.VARLONG.fixedSize());
        int off = (int)offLen;
        int len = (int)(offLen >>> 32);

        return readString(off, len);
    }

    private void checkColumn(Columns cols, int idx, NativeType type) {
        Column col = cols.column(idx);

        if (!col.type().equals(type))
            throw new IllegalArgumentException("Invalid column type requested [requested=" + type +
                ", column=" + col + ']');
    }

    private int baseOffset(SchemaDescriptor schema, int col) {
        boolean keyCol = schema.keyColumn(col);

        int startOff = SCHEMA_VERSION_FIELD_SIZE + KEY_HASH_FIELD_SIZE;

        if (keyCol)
            return startOff;

        int keyLen = readShort(startOff);

        return startOff + keyLen;
    }

    protected int indexFromBase(SchemaDescriptor schema, int col) {
        if (schema.keyColumn(col))
            return col;

        return col - schema.keyColumns().length();
    }

    private boolean isNull(int baseOff, int idx) {
        int nullMapOff = nullMapOffset(baseOff);

        int nullByte = idx / 8;
        int posInByte = idx % 8;

        int map = readByte(nullMapOff + nullByte);

        return (map & (1 << posInByte)) != 0;
    }

    private long varlenColumnOffsetAndLength(Columns cols, int baseOff, int idx, boolean fixlen) {
        int nullMapOff = nullMapOffset(baseOff);

        int nullStartByte = cols.firstVarsizeColumn() / 8;
        int startBitInByte = cols.firstVarsizeColumn() % 8;

        int nullEndByte = idx / 8;
        int endBitInByte = idx % 8;
        int numNullsBefore = 0;

        for (int i = nullStartByte; i <= nullEndByte; i++) {
            int nullmapByte = readByte(nullMapOff + i);

            if (i == nullStartByte)
                // We need to clear startBitInByte least significant bits
                nullmapByte &= (0xFF << startBitInByte);

            if (i == nullEndByte)
                // We need to clear 8-endBitInByte most significant bits
                nullmapByte &= (0xFF >> (8 - endBitInByte));

            numNullsBefore += Columns.numberOfNullColumns(nullmapByte);
        }

        idx -= cols.numberOfFixsizeColumns() + numNullsBefore;
        int vartableSize = readShort(baseOff + TOTAL_LEN_FIELD_SIZE);

        int vartableOff = vartableOffset(baseOff);
        // Offset of idx-th column is from base offset.
        int resOff = readShort(vartableOff + 2 * idx);

        long len = idx == vartableSize - 1 ?
            // totalLength - columnStartOffset
            readShort(baseOff) - resOff:
            // nextColumnStartOffset - columnStartOffset
            readShort(vartableOff + 2 * (idx + 1)) - resOff;

        return (len << 32) | (resOff + baseOff);
    }

    int fixlenColumnOffset(Columns cols, int baseOff, int idx) {
        int nullMapOff = nullMapOffset(baseOff);

        int off = 0;
        int nullMapIdx = idx / 8;

        // Fold offset based on the whole map bytes in the schema
        for (int i = 0; i < nullMapIdx; i++)
            off += cols.foldFixedLength(i, readByte(nullMapOff + i));

        // Set bits starting from posInByte, inclusive, up to either the end of the byte or the last column index, inclusive
        int startBit = idx % 8;
        int endBit = nullMapIdx == cols.nullMapSize() - 1 ? ((cols.numberOfFixsizeColumns() - 1) % 8) : 7;
        int mask = (0xFF >> (7 - endBit)) & (0xFF << startBit);

        off += cols.foldFixedLength(nullMapIdx, readByte(nullMapOff + nullMapIdx) | mask);

        return nullMapOff + cols.nullMapSize() + off;
    }

    private int nullMapOffset(int baseOff) {
        int varlenTableLen = readShort(baseOff + TOTAL_LEN_FIELD_SIZE) * 2;

        return vartableOffset(baseOff) + varlenTableLen;
    }

    private int vartableOffset(int baseOff) {
        return baseOff + TOTAL_LEN_FIELD_SIZE + VARSIZE_TABLE_LEN_FIELD_SIZE;
    }

    protected abstract String readString(int off, int len);

    protected abstract long readVarlong(int off);

    protected abstract long readLong(int off);

    protected abstract int readInteger(int off);

    protected abstract int readShort(int off);

    protected abstract int readByte(int off);
}

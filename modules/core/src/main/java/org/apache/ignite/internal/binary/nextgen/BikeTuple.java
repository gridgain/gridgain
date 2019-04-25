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

package org.apache.ignite.internal.binary.nextgen;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.binary.BinaryPrimitives;

public class BikeTuple {
    // t0d0 int, long, word element?
    private static final byte[] NTBL = {8, 7, 7, 6, 7, 6, 6, 5, 7, 6, 6, 5, 6, 5, 5, 4, 7, 6, 6, 5, 6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3, 7, 6, 6, 5, 6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3, 6, 5, 5, 4, 5, 4, 4, 3, 5, 4, 4, 3, 4, 3, 3, 2, 7, 6, 6, 5, 6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3, 6, 5, 5, 4, 5, 4, 4, 3, 5, 4, 4, 3, 4, 3, 3, 2, 6, 5, 5, 4, 5, 4, 4, 3, 5, 4, 4, 3, 4, 3, 3, 2, 5, 4, 4, 3, 4, 3, 3, 2, 4, 3, 3, 2, 3, 2, 2, 1, 7, 6, 6, 5, 6, 5, 5, 4, 6, 5, 5, 4, 5, 4, 4, 3, 6, 5, 5, 4, 5, 4, 4, 3, 5, 4, 4, 3, 4, 3, 3, 2, 6, 5, 5, 4, 5, 4, 4, 3, 5, 4, 4, 3, 4, 3, 3, 2, 5, 4, 4, 3, 4, 3, 3, 2, 4, 3, 3, 2, 3, 2, 2, 1, 6, 5, 5, 4, 5, 4, 4, 3, 5, 4, 4, 3, 4, 3, 3, 2, 5, 4, 4, 3, 4, 3, 3, 2, 4, 3, 3, 2, 3, 2, 2, 1, 5, 4, 4, 3, 4, 3, 3, 2, 4, 3, 3, 2, 3, 2, 2, 1, 4, 3, 3, 2, 3, 2, 2, 1, 3, 2, 2, 1, 2, 1, 1, 0};

    private final byte[] data;

    public BikeTuple(byte[] data) {
        this.data = data;
    }

    public Object attr(int i, Class<?> type, int intType) {
        // t0d0 worth adding special method for extracting needed subset of columns
        int nOff = nullsOffset();

        int nullChunkPos = i >>> 3;
        byte nullChunk = data[nOff + 1 + nullChunkPos];
        int posInChunk = i % 8;

        if (((nullChunk >>> posInChunk) & 1) == 0)
            return null;

        int lenOff = nOff + data[nOff] & 0xFF;

        int off = lenOff + BinaryPrimitives.readShort(data, lenOff);

        lenOff+=2;

        int nonNullsBefore = i;
        int j = 0;
        while (j < nullChunkPos) {
            byte chunk;
            if ((chunk = data[nOff + 1 + j]) != -1) {
                // search for nulls before
                nonNullsBefore -= NTBL[chunk & 0xFF];
            }

            j++;
        }
        if (nullChunk != - 1) {
            for (int k = 0; k < posInChunk; k++) {
                // t0d0 mask and lookup table
                if (((nullChunk >>> k) & 1) == 0)
                    nonNullsBefore--;
            }
        }

        int len = 0;

//        for (int k = 0; k <= nonNullsBefore; k++) {
//            off += len;
//
//            len = BinaryPrimitives.readShort(data, lenOff);
//            lenOff+=2;
//        }

        int offoff = lenOff + 2 * nonNullsBefore;
        short off0 = BinaryPrimitives.readShort(data, offoff);
        short nextOff = offoff == (off - 2) ? (short)(data.length - off) : BinaryPrimitives.readShort(data, offoff + 2);
        off += off0;
        len = nextOff - off0;

        return deserializeValue(data, off, len, type, intType);
    }

    public int attrInt(int i) {
        // t0d0 worth adding special method for extracting needed subset of columns
        int nOff = nullsOffset();

        int lenOff = nOff + data[nOff] & 0xFF;

        byte nullChunk = data[nOff + 1 + (i >>> 3)];
        int posInChunk = i % 8;

        if (((nullChunk >>> posInChunk) & 1) == 0)
            return 0;

        int off = lenOff + BinaryPrimitives.readShort(data, lenOff);

        lenOff+=2;

        int nonNullsBefore = i;
        int j = 0;
        while (j < (i >>> 3)) {
            byte chunk;
            if ((chunk = data[nOff + 1 + j]) != -1) {
                // search for nulls before
                for (int k = 0; k < 8; k++) {
                    if (((chunk >>> k) & 1) == 0)
                        nonNullsBefore--;
                }
            }

            j++;
        }
        for (int k = 0; k < posInChunk; k++) {
            if (((nullChunk >>> k) & 1) == 0)
                nonNullsBefore--;
        }

        int len = 0;

//        for (int k = 0; k <= nonNullsBefore; k++) {
//            off += len;
//
//            len = BinaryPrimitives.readShort(data, lenOff);
//            lenOff+=2;
//        }

        int offoff = lenOff + 2 * nonNullsBefore;
        short off0 = BinaryPrimitives.readShort(data, offoff);
        short nextOff = offoff == (off - 2) ? (short)(data.length - off) : BinaryPrimitives.readShort(data, offoff + 2);
        off += off0;
        len = nextOff - off0;

        return BinaryPrimitives.readInt(data, off);
    }

    public long attrLong(int i) {
        // t0d0 worth adding special method for extracting needed subset of columns
        int nOff = nullsOffset();

        int lenOff = nOff + data[nOff] & 0xFF;

        byte nullChunk = data[nOff + 1 + (i >>> 3)];
        int posInChunk = i % 8;

        if (((nullChunk >>> posInChunk) & 1) == 0)
            return 0L;

        int off = lenOff + BinaryPrimitives.readShort(data, lenOff);

        lenOff+=2;

        int nonNullsBefore = i;
        int j = 0;
        while (j < (i >>> 3)) {
            byte chunk;
            if ((chunk = data[nOff + 1 + j]) != -1) {
                // search for nulls before
                for (int k = 0; k < 8; k++) {
                    if (((chunk >>> k) & 1) == 0)
                        nonNullsBefore--;
                }
            }

            j++;
        }
        for (int k = 0; k < posInChunk; k++) {
            if (((nullChunk >>> k) & 1) == 0)
                nonNullsBefore--;
        }

        int len = 0;

//        for (int k = 0; k <= nonNullsBefore; k++) {
//            off += len;
//
//            len = BinaryPrimitives.readShort(data, lenOff);
//            lenOff+=2;
//        }

        int offoff = lenOff + 2 * nonNullsBefore;
        short off0 = BinaryPrimitives.readShort(data, offoff);
        short nextOff = offoff == (off - 2) ? (short)(data.length - off) : BinaryPrimitives.readShort(data, offoff + 2);
        off += off0;
        len = nextOff - off0;

        return BinaryPrimitives.readLong(data, off);
    }

    private Object deserializeValue(byte[] data, int off, int len, Class<?> type, int intType) {
        // t0d0 more clever type conversion
        if (type == String.class)
//        if (intType == 4)
            return new String(data, off, len, StandardCharsets.UTF_8);

        if (type == Integer.class)
//        if (intType == 1)
            return BinaryPrimitives.readInt(data, off);

        if (type == Long.class)
//        if (intType == 2)
            return BinaryPrimitives.readLong(data, off);

        if (type == BigDecimal.class) {
//        if (intType == 3) {
            return new BigDecimal(
                new BigInteger(BinaryPrimitives.readByteArray(data, off + 4, len - 4)),
                BinaryPrimitives.readInt(data, off));
        }

        throw new RuntimeException();
    }

    private int nullsOffset() {
        return 4;
    }

    public byte[] data() {
        return data;
    }

    public static int intType(Class<?> type) {
        if (type == Integer.class)
            return 1;
        if (type == Long.class)
            return 2;
        if (type == BigDecimal.class)
            return 3;
        if (type == String.class)
            return 4;

        return 0;
    }

    public int typeId() {
        return BinaryPrimitives.readInt(data, 0);
    }
}

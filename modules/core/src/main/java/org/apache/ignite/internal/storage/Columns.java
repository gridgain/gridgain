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
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class Columns {
    private static final int[] NULL_COLUMNS_LOOKUP;

    private final Column[] cols;

    private final int firstVarsizeColIdx;

    private final int nullMapSize;

    private int[][] foldingTable;
    private int[] foldingMask;

    static {
        NULL_COLUMNS_LOOKUP = new int[256];

        // Each nonzero bit is a null value.
        for (int i = 0; i < 255; i++)
            NULL_COLUMNS_LOOKUP[i] = Integer.bitCount(i);
    }

    public static int numberOfNullColumns(int nullmapByte) {
        return NULL_COLUMNS_LOOKUP[nullmapByte];
    }

    public Columns(Column... cols) {
        this.cols = sortedCopy(cols);

        firstVarsizeColIdx = findFirstVarsizeColumn();

        nullMapSize = (cols.length + 7) / 8;

        buildFoldingTable();
    }

    /**
     * Calculates a sum of fixed-sized columns lengths given the mask of the present columns, assuming that the
     * mask byte is an {@code i}-th byte is columns mask.
     *
     * @param i Mask byte index in the null-map.
     * @param maskByte Mask byte value, where a nonzero bit (counting from LSB to MSB) represents a {@code null} value
     *      and the corresponding column length should be skipped.
     * @return Fixed columns length sizes summed wrt to the mask.
     */
    public int foldFixedLength(int i, int maskByte) {
        return foldingTable[i][maskByte & foldingMask[i]];
    }

    public int nullMapSize() {
        return nullMapSize;
    }

    public boolean isFixedSize(int idx) {
        return firstVarsizeColIdx == -1 || idx < firstVarsizeColIdx;
    }

    public Column column(int idx) {
        return cols[idx];
    }

    public int length() {
        return cols.length;
    }

    public int firstVarsizeColumn() {
        return firstVarsizeColIdx;
    }

    public int numberOfFixsizeColumns() {
        return firstVarsizeColIdx == -1 ? cols.length : firstVarsizeColIdx;
    }

    public int numberOfVarsizeColumns() {
        return cols.length - numberOfFixsizeColumns();
    }

    private Column[] sortedCopy(Column[] cols) {
        Column[] cp = Arrays.copyOf(cols, cols.length);

        Arrays.sort(cp);

        return cp;
    }

    private int findFirstVarsizeColumn() {
        for (int i = 0; i < cols.length; i++) {
            if (!cols[i].type().fixedSize())
                return i;
        }

        return -1;
    }

    private void buildFoldingTable() {
        int numFixsize = numberOfFixsizeColumns();

        if (numFixsize == 0) {
            foldingTable = new int[0][];
            foldingMask = new int[0];

            return;
        }

        int fixsizeNullMapSize = (numFixsize + 7) / 8;

        int[][] res = new int[fixsizeNullMapSize][];
        int[] resMask = new int[fixsizeNullMapSize];

        for (int b = 0; b < fixsizeNullMapSize; b++) {
            int bitsInMask = b == fixsizeNullMapSize - 1 ?
                (numFixsize - 8 * b) : 8;

            int totalMasks = 1 << bitsInMask;

            resMask[b] = 0xFF >>> (8 - bitsInMask);

            res[b] = new int[totalMasks];

            // Start with all non-nulls.
            int mask = 0x00;

            for (int i = 0; i < totalMasks; i++) {
                res[b][mask] = foldManual(b, mask);

                mask++;
            }
        }

        foldingTable = res;
        foldingMask = resMask;
    }

    private int foldManual(int b, int mask) {
        int size = 0;

        for (int bit = 0; bit < 8; bit++) {
            boolean hasValue = (mask & (1 << bit)) == 0;

            int idx = b * 8 + bit;

            if (hasValue && idx < numberOfFixsizeColumns()) {
                assert cols[idx].type().fixedSize() : "Expected fixed-size column [b=" + b +
                    ", mask=" + U.hexInt(mask) +
                    ", cols" + Arrays.toString(cols) + ']';

                size += cols[idx].type().size();
            }
        }

        return size;
    }
}

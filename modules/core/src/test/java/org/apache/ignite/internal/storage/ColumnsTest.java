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

import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class ColumnsTest {
    @Test
    public void testFixsizeIndex() {
        Columns cols = new Columns(
            new Column("intCol2", NativeType.INTEGER, false),
            new Column("intCol1", NativeType.INTEGER, false),
            new Column("uuidCol", NativeType.UUID, false)
        );

        assertEquals(3, cols.length());
        assertEquals(-1, cols.firstVarsizeColumn());

        for (int c = 0; c < cols.length(); c++)
            assertTrue(cols.isFixedSize(c));

        assertEquals(1, cols.nullMapSize());
        assertEquals(3, cols.numberOfFixsizeColumns());
    }

    @Test
    public void testVarsizeIndex() {
        Columns cols = new Columns(
            new Column("stringCol3", NativeType.STRING, false),
            new Column("stringCol2", NativeType.STRING, false),
            new Column("stringCol1", NativeType.STRING, false)
        );

        assertEquals(3, cols.length());
        assertEquals(0, cols.firstVarsizeColumn());

        for (int c = 0; c < cols.length(); c++)
            assertFalse(cols.isFixedSize(c));

        assertEquals(1, cols.nullMapSize());
        assertEquals(0, cols.numberOfFixsizeColumns());
    }

    @Test
    public void testMixedIndex() {
        Columns cols = new Columns(
            new Column("stringCol", NativeType.STRING, false),
            new Column("intCol2", NativeType.INTEGER, false),
            new Column("intCol1", NativeType.INTEGER, false),
            new Column("uuidCol", NativeType.UUID, false)
        );

        assertEquals(4, cols.length());
        assertEquals(3, cols.firstVarsizeColumn());

        for (int c = 0; c < cols.length(); c++) {
            if (c < cols.firstVarsizeColumn())
                assertTrue(cols.isFixedSize(c));
            else
                assertFalse(cols.isFixedSize(c));
        }

        assertEquals(1, cols.nullMapSize());
        assertEquals(3, cols.numberOfFixsizeColumns());
    }

    @Test
    public void testNullMapSize() {
        assertEquals(1, new Columns(columns(1)).nullMapSize());
        assertEquals(1, new Columns(columns(7)).nullMapSize());
        assertEquals(1, new Columns(columns(8)).nullMapSize());

        assertEquals(2, new Columns(columns(9)).nullMapSize());
        assertEquals(2, new Columns(columns(10)).nullMapSize());
        assertEquals(2, new Columns(columns(15)).nullMapSize());
        assertEquals(2, new Columns(columns(16)).nullMapSize());

        assertEquals(3, new Columns(columns(17)).nullMapSize());
        assertEquals(3, new Columns(columns(18)).nullMapSize());
        assertEquals(3, new Columns(columns(23)).nullMapSize());
        assertEquals(3, new Columns(columns(24)).nullMapSize());
    }

    @Test
    public void testFoldSizeNoVarlenIncomplete1Byte() {
        Column[] colDef = {
            new Column("a", NativeType.DATE, false),    // 3
            new Column("b", NativeType.INTEGER, false), // 4
            new Column("c", NativeType.INTEGER, false), // 4
            new Column("d", NativeType.TIME, false),    // 5
            new Column("e", NativeType.TIME, false),    // 5
            new Column("f", NativeType.LONG, false),    // 8
            new Column("g", NativeType.UUID, false)     // 16
        };

        checkColumnFolding(colDef);
    }

    @Test
    public void testFoldSizeNoVarlenFull1Byte() {
        Column[] colDef = {
            new Column("a", NativeType.DATE, false),    // 3
            new Column("b", NativeType.INTEGER, false), // 4
            new Column("c", NativeType.INTEGER, false), // 4
            new Column("d", NativeType.TIME, false),    // 5
            new Column("e", NativeType.TIME, false),    // 5
            new Column("f", NativeType.LONG, false),    // 8
            new Column("g", NativeType.UUID, false),    // 16
            new Column("h", NativeType.UUID, false)     // 16
        };

        checkColumnFolding(colDef);
    }

    @Test
    public void testFoldSizeNoVarlenIncomplete2Bytes() {
        Column[] colDef = {
            new Column("a", NativeType.DATE, false),    // 3
            new Column("b", NativeType.DATE, false),    // 3
            new Column("c", NativeType.INTEGER, false), // 4
            new Column("d", NativeType.INTEGER, false), // 4
            new Column("e", NativeType.INTEGER, false), // 4
            new Column("f", NativeType.TIME, false),    // 5
            new Column("g", NativeType.TIME, false),    // 5
            new Column("h", NativeType.LONG, false),    // 8
            new Column("i", NativeType.UUID, false),    // 16
            new Column("j", NativeType.UUID, false)     // 16
        };

        checkColumnFolding(colDef);
    }

    @Test
    public void testFoldSizeNoVarlenFull2Bytes() {
        Column[] colDef = {
            new Column("a", NativeType.DATE, false),    // 3
            new Column("b", NativeType.DATE, false),    // 3
            new Column("c", NativeType.DATE, false),    // 3
            new Column("d", NativeType.DATE, false),    // 3
            new Column("e", NativeType.INTEGER, false), // 4
            new Column("f", NativeType.INTEGER, false), // 4
            new Column("g", NativeType.INTEGER, false), // 4
            new Column("h", NativeType.TIME, false),    // 5
            new Column("i", NativeType.TIME, false),    // 5
            new Column("j", NativeType.TIME, false),    // 5
            new Column("k", NativeType.LONG, false),    // 8
            new Column("l", NativeType.LONG, false),    // 8
            new Column("m", NativeType.LONG, false),    // 8
            new Column("n", NativeType.UUID, false),    // 16
            new Column("o", NativeType.UUID, false),    // 16
            new Column("p", NativeType.UUID, false)     // 16
        };

        checkColumnFolding(colDef);
    }

    @Test
    public void testFoldSizeVarlenIncomplete1Byte() {
        Column[] colDef = {
            new Column("a", NativeType.DATE, false),    // 3
            new Column("b", NativeType.INTEGER, false), // 4
            new Column("c", NativeType.INTEGER, false), // 4
            new Column("d", NativeType.TIME, false),    // 5
            new Column("e", NativeType.TIME, false),    // 5
            new Column("f", NativeType.STRING, false),
            new Column("g", NativeType.VARLONG, false)
        };

        checkColumnFolding(colDef);
    }

    @Test
    public void testFoldSizeVarlenFull1Byte() {
        Column[] colDef = {
            new Column("a", NativeType.DATE, false),    // 3
            new Column("b", NativeType.INTEGER, false), // 4
            new Column("c", NativeType.INTEGER, false), // 4
            new Column("d", NativeType.TIME, false),    // 5
            new Column("e", NativeType.TIME, false),    // 5
            new Column("f", NativeType.STRING, false),
            new Column("g", NativeType.STRING, false),
            new Column("h", NativeType.VARLONG, false)
        };

        checkColumnFolding(colDef);
    }

    @Test
    public void testFoldSizeVarlenIncomplete2Bytes1() {
        Column[] colDef = {
            new Column("a", NativeType.DATE, false),    // 3
            new Column("b", NativeType.INTEGER, false), // 4
            new Column("c", NativeType.INTEGER, false), // 4
            new Column("d", NativeType.TIME, false),    // 5
            new Column("e", NativeType.TIME, false),    // 5
            new Column("f", NativeType.TIME, false),    // 5
            new Column("g", NativeType.STRING, false),
            new Column("h", NativeType.STRING, false),
            new Column("i", NativeType.VARLONG, false)
        };

        checkColumnFolding(colDef);
    }

    @Test
    public void testFoldSizeVarlenIncomplete2Bytes2() {
        Column[] colDef = {
            new Column("a", NativeType.DATE, false),    // 3
            new Column("b", NativeType.INTEGER, false), // 4
            new Column("c", NativeType.INTEGER, false), // 4
            new Column("d", NativeType.INTEGER, false), // 4
            new Column("e", NativeType.TIME, false),    // 5
            new Column("f", NativeType.TIME, false),    // 5
            new Column("g", NativeType.TIME, false),    // 5
            new Column("h", NativeType.TIME, false),    // 5
            new Column("i", NativeType.STRING, false),
            new Column("j", NativeType.STRING, false),
            new Column("k", NativeType.VARLONG, false)
        };

        checkColumnFolding(colDef);
    }

    @Test
    public void testFoldSizeVarlenIncomplete2Bytes3() {
        Column[] colDef = {
            new Column("a", NativeType.DATE, false),    // 3
            new Column("b", NativeType.INTEGER, false), // 4
            new Column("c", NativeType.INTEGER, false), // 4
            new Column("d", NativeType.INTEGER, false), // 4
            new Column("e", NativeType.TIME, false),    // 5
            new Column("f", NativeType.TIME, false),    // 5
            new Column("g", NativeType.TIME, false),    // 5
            new Column("h", NativeType.TIME, false),    // 5
            new Column("i", NativeType.TIME, false),    // 5
            new Column("j", NativeType.STRING, false),
            new Column("k", NativeType.VARLONG, false)
        };

        checkColumnFolding(colDef);
    }

    @Test
    public void testFoldSizeVarlenFull2Bytes() {
        Column[] colDef = {
            new Column("a", NativeType.DATE, false),    // 3
            new Column("b", NativeType.INTEGER, false), // 4
            new Column("c", NativeType.INTEGER, false), // 4
            new Column("d", NativeType.INTEGER, false), // 4
            new Column("e", NativeType.TIME, false),    // 5
            new Column("f", NativeType.TIME, false),    // 5
            new Column("g", NativeType.TIME, false),    // 5
            new Column("h", NativeType.TIME, false),    // 5
            new Column("i", NativeType.TIME, false),    // 5
            new Column("j", NativeType.STRING, false),
            new Column("k", NativeType.VARLONG, false),
            new Column("l", NativeType.VARLONG, false),
            new Column("m", NativeType.VARLONG, false),
            new Column("n", NativeType.VARLONG, false),
            new Column("o", NativeType.VARLONG, false),
            new Column("p", NativeType.VARLONG, false)
        };

        checkColumnFolding(colDef);
    }

    private void checkColumnFolding(Column[] colDef) {
        Columns cols = new Columns(colDef);

        boolean[] nullMasks = new boolean[cols.numberOfFixsizeColumns()];

        for (int i = 0; i < (1 << cols.numberOfFixsizeColumns()); i++) {
            checkSize(cols, colDef, nullMasks);

            incrementMask(nullMasks);
        }
    }

    private void incrementMask(boolean[] mask) {
        boolean add = true;

        for (int i = 0; i < mask.length && add; i++) {
            add = mask[i];
            mask[i] = !mask[i];
        }
    }

    private void checkSize(Columns cols, Column[] colDef, boolean[] nullMasks) {
        // Iterate over bytes first
        for (int b = 0; b < (cols.numberOfFixsizeColumns() + 7) / 8; b++) {
            // Start with all non-nulls.
            int mask = 0x00;
            int size = 0;

            for (int bit = 0; bit < 8; bit++) {
                int idx = 8 * b + bit;

                if (idx >= cols.numberOfFixsizeColumns())
                    break;

                assertTrue(colDef[idx].type().fixedSize());

                if (nullMasks[idx])
                    // set bit in the mask (indicate null value).
                    mask |= (1 << bit);
                else
                    // non-null, sum the size.
                    size += colDef[idx].type().size();
            }

            assertEquals("Failed [b=" + b + ", mask=" + U.hexInt(mask) + ']',
                size, cols.foldFixedLength(b, mask));
        }
    }

    private static Column[] columns(int size) {
        Column[] ret = new Column[size];

        for (int i = 0; i < ret.length; i++)
            ret[i] = new Column("column-" + i, NativeType.STRING, true);

        return ret;
    }
}

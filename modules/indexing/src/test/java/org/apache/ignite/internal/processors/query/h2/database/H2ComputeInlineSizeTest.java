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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.BytesInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.LongInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.StringInlineIndexColumn;
import org.h2.table.Column;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.junit.Test;

/** Tests for the computation of default inline size */
public class H2ComputeInlineSizeTest extends AbstractIndexingCommonTest {

    /**
     * Default variable length of the column's index
     */
    private static final int DEFAULT_VARIABLE_LENGTH = 10;

    /**
     * Default max length of the default index
     */
    private static final int DEFAULT_MAX_LENGTH = 64;

    /**
     * Test to check calculation of the default size for StringInlineIndexColumn
     *
     * Steps:
     * 1) Call H2TreeIndexBase.computeInlineSize() function for StringInlineIndexColumn
     * 2) Check that computed size is equal to default length for variable types
     */
    @Test
    public void testDefaultSizeForString() {
        Column c = new Column("c", new TypeInfo(Value.STRING, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(new StringInlineIndexColumn(c, false));

        assertEquals(DEFAULT_VARIABLE_LENGTH, H2TreeIndexBase.computeInlineSize(inlineIdxs, -1, -1));
    }

    /**
     * Test to check calculation of the default size for StringInlineIndexColumn
     *
     * Steps:
     * 1) Call H2TreeIndexBase.computeInlineSize() function for BytesInlineIndexColumn
     * 2) Check that computed size is equal to default length for variable types
     */
    @Test
    public void testDefaultSizeForByteArray() {
        Column c = new Column("c", new TypeInfo(Value.BYTES, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(new BytesInlineIndexColumn(c, false));

        assertEquals(DEFAULT_VARIABLE_LENGTH, H2TreeIndexBase.computeInlineSize(inlineIdxs, -1, -1));
    }

    /**
     * Test to check calculation of the default size for StringInlineIndexColumn with defined length
     *
     * Steps:
     * 1) Call H2TreeIndexBase.computeInlineSize() function for StringInlineIndexColumn with defined length
     * 2) Check that computed size is equal to defined length
     */
    @Test
    public void testDefaultSizeForStringWithDefinedLength() {
        final byte LEN = DEFAULT_VARIABLE_LENGTH + 10;

        Column c = new Column("c", new TypeInfo(Value.STRING, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
        c.setOriginalSQL("VARCHAR(" + LEN + ")");

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(new StringInlineIndexColumn(c, false));

        assertEquals(LEN, H2TreeIndexBase.computeInlineSize(inlineIdxs, -1, -1));
    }

    /**
     * Test to check calculation of the default size for StringInlineIndexColumn with unexpected sql pattern
     *
     * Steps:
     * 1) Call H2TreeIndexBase.computeInlineSize() function for StringInlineIndexColumn with unexpected sql pattern
     * 2) Check that computed size is equal to default length for variable types
     */
    @Test
    public void testDefaultSizeForStringWithIncorrectSql() {
        Column c = new Column("c", new TypeInfo(Value.STRING, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
        c.setOriginalSQL("CHAR()");

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(new StringInlineIndexColumn(c, false));

        assertEquals(DEFAULT_VARIABLE_LENGTH, H2TreeIndexBase.computeInlineSize(inlineIdxs, -1, -1));
    }

    /**
     * Test to check calculation of the default size for composite index
     *
     * Steps:
     * 1) Call H2TreeIndexBase.computeInlineSize() function for 2 StringInlineIndexColumns, 1 BytesInlineIndexColumns
     * and 1 LongInlineIndexColumn
     * 2) Check that computed size is equal to 3 * default length for variable types + constant length of long column
     */
    @Test
    public void testDefaultSizeForCompositeIndex() {
        Column c1 = new Column("c1", new TypeInfo(Value.STRING, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
        Column c2 = new Column("c2", new TypeInfo(Value.STRING, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
        Column c3 = new Column("c3", new TypeInfo(Value.BYTES, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
        Column c4 = new Column("c4", new TypeInfo(Value.LONG, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));

        InlineIndexColumn lCol = new LongInlineIndexColumn(c4);

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(new StringInlineIndexColumn(c1, false));
        inlineIdxs.add(new StringInlineIndexColumn(c2, false));
        inlineIdxs.add(new BytesInlineIndexColumn(c3, false));
        inlineIdxs.add(lCol);

        assertEquals(3 * DEFAULT_VARIABLE_LENGTH + lCol.size() + 1, H2TreeIndexBase.computeInlineSize(inlineIdxs, -1, -1));
    }

    /**
     * Test to check calculation of the default size when calculated default size is larger than max default index size
     *
     * Steps:
     * 1) Call H2TreeIndexBase.computeInlineSize() function for StringInlineIndexColumn with large length
     * 2) Check that computed size is equal to default max length
     */
    @Test
    public void testDefaultSizeForLargeIndex() {
        Column c = new Column("c", new TypeInfo(Value.STRING, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
        c.setOriginalSQL("VARCHAR(" + 300 + ")");

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(new StringInlineIndexColumn(c, false));

        assertEquals(DEFAULT_MAX_LENGTH, H2TreeIndexBase.computeInlineSize(inlineIdxs, -1, -1));
    }
}

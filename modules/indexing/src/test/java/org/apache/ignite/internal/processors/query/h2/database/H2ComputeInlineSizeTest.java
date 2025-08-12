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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.BooleanInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.ByteInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.BytesInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.DateInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.DecimalInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.DoubleInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.FixedStringInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.FloatInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.IntegerInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.LongInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.ObjectHashInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.ShortInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.StringIgnoreCaseInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.StringInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.TimeInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.TimestampInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.UuidInlineIndexColumn;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.gridgain.internal.h2.table.Column;
import org.gridgain.internal.h2.value.TypeInfo;
import org.gridgain.internal.h2.value.Value;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE;
import static org.apache.ignite.internal.util.IgniteUtils.MAX_INLINE_SIZE;

/** Tests for the computation of default inline size. */
public class H2ComputeInlineSizeTest extends AbstractIndexingCommonTest {

    /**
     * Test to check calculation of the default size for {@link StringInlineIndexColumn}.
     *
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(String, java.util.List, int, int, org.apache.ignite.IgniteLogger)} function for {@link StringInlineIndexColumn}.
     * 2) Check that computed size is equal to default length for variable types.
     */
    @Test
    public void testDefaultSizeForString() {
        Column c = new Column("c", new TypeInfo(Value.STRING, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(createHelper(c, false));

        assertEquals(H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE, H2TreeIndexBase.computeInlineSize("idx", inlineIdxs, -1, -1, log));
    }

    /**
     * Test to check calculation of the default size for {@link BytesInlineIndexColumn}.
     *
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(String, java.util.List, int, int, org.apache.ignite.IgniteLogger)} function for {@link BytesInlineIndexColumn}.
     * 2) Check that computed size is equal to default length for variable types.
     */
    @Test
    public void testDefaultSizeForByteArray() {
        Column c = new Column("c", new TypeInfo(Value.BYTES, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(createHelper(c, false));

        assertEquals(H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE, H2TreeIndexBase.computeInlineSize("idx", inlineIdxs, -1, -1, log));
    }

    /**
     * Test to check calculation of the default size for {@link StringInlineIndexColumn} with defined length.
     *
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(String, java.util.List, int, int, org.apache.ignite.IgniteLogger)} function for {@link StringInlineIndexColumn} with defined length.
     * 2) Check that computed size is equal to defined length + 3 bytes (inner system info for String type).
     */
    @Test
    public void testDefaultSizeForStringWithDefinedLength() {
        final byte LEN = H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE + 10;

        Column c = new Column("c", new TypeInfo(Value.STRING, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
        c.setOriginalSQL("VARCHAR(" + LEN + ")");

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(createHelper(c, false));

        assertEquals(LEN + 3, H2TreeIndexBase.computeInlineSize("idx", inlineIdxs, -1, -1, log));
    }

    /**
     * Test to check calculation of the default size for {@link BytesInlineIndexColumn} with defined length.
     *
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(String, java.util.List, int, int, org.apache.ignite.IgniteLogger)} function for {@link BytesInlineIndexColumn} with defined length.
     * 2) Check that computed size is equal to defined length + 3 bytes (inner system info for byte[] type).
     */
    @Test
    public void testDefaultSizeForBytesWithDefinedLength() {
        final byte LEN = H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE + 20;

        Column c = new Column("c", new TypeInfo(Value.BYTES, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
        c.setOriginalSQL("BINARY(" + LEN + ")");

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(createHelper(c, false));

        assertEquals(LEN + 3, H2TreeIndexBase.computeInlineSize("idx", inlineIdxs, -1, -1, log));
    }

    /**
     * Test to check calculation of the default size for {@link StringInlineIndexColumn} with unexpected sql pattern.
     *
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(String, java.util.List, int, int, org.apache.ignite.IgniteLogger)} function for {@link StringInlineIndexColumn} with unexpected sql pattern.
     * 2) Check that computed size is equal to default length for variable types.
     */
    @Test
    public void testDefaultSizeForStringWithIncorrectSql() {
        Column c = new Column("c", new TypeInfo(Value.STRING, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
        c.setOriginalSQL("CHAR()");

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(createHelper(c, false));

        assertEquals(H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE, H2TreeIndexBase.computeInlineSize("idx", inlineIdxs, -1, -1, log));
    }

    /**
     * Test to check calculation of the default size for composite index.
     *
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(String, java.util.List, int, int, org.apache.ignite.IgniteLogger)} function for {@link StringInlineIndexColumn},
     * {@link BytesInlineIndexColumn}, {@link LongInlineIndexColumn} and {@link StringInlineIndexColumn} with defined length.
     * 2) Check that computed size is equal to 2 * default length for variable types + constant length of long column +
     * defined String length + 3 bytes (inner system info for String type).
     */
    @Test
    public void testDefaultSizeForCompositeIndex() {
        final byte LEN = H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE - 1;

        Column c1 = new Column("c1", new TypeInfo(Value.STRING, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
        Column c2 = new Column("c2", new TypeInfo(Value.BYTES, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
        Column c3 = new Column("c3", new TypeInfo(Value.LONG, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));

        Column c4 = new Column("c4", new TypeInfo(Value.STRING, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
        c4.setOriginalSQL("VARCHAR(" + LEN + ")");

        InlineIndexColumn lCol = new LongInlineIndexColumn(c3);

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(createHelper(c1, false));
        inlineIdxs.add(createHelper(c2, false));
        inlineIdxs.add(createHelper(c3, false));
        inlineIdxs.add(createHelper(c4, false));

        assertEquals(2 * H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE + lCol.size() + 1 + LEN + 3,
            H2TreeIndexBase.computeInlineSize("idx", inlineIdxs, -1, -1, log));
    }

    /**
     * Test to check calculation of the default size when calculated default size is larger than max index size.
     *
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(String, java.util.List, int, int, org.apache.ignite.IgniteLogger)} function for {@link StringInlineIndexColumn} with large length.
     * 2) Check that computed size is equal to max size.
     */
    @Test
    @WithSystemProperty(key = IGNITE_MAX_INDEX_PAYLOAD_SIZE, value = Integer.MAX_VALUE + "")
    public void testMaximumSizeUsedForLargeIndex() {
        Column c = new Column("c", new TypeInfo(Value.STRING, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
        c.setOriginalSQL("VARCHAR(" + (MAX_INLINE_SIZE + 1) + ")");

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(createHelper(c, false));

        assertEquals(MAX_INLINE_SIZE, H2TreeIndexBase.computeInlineSize("idx", inlineIdxs, -1, -1, log));
    }

    /**
     * Test to check that maximum configured max size is used when exceeded.
     * <p>
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(String, java.util.List, int, int, org.apache.ignite.IgniteLogger)} function for {@link StringInlineIndexColumn} with large length.
     * 2) Check that computed size is equal to max size.
     */
    @Test
    @WithSystemProperty(key = IGNITE_MAX_INDEX_PAYLOAD_SIZE, value = Integer.MAX_VALUE + "")
    public void testConfiguredMaximumSizeUsedForLargeIndex() {
        int cfgMaxSize = 100;

        Column c = new Column("c", new TypeInfo(Value.STRING, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
        c.setOriginalSQL("VARCHAR(" + (cfgMaxSize + 1) + ")");

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(createHelper(c, false));

        assertEquals(cfgMaxSize, H2TreeIndexBase.computeInlineSize("idx", inlineIdxs, -1, cfgMaxSize, log));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_MAX_INDEX_PAYLOAD_SIZE, value = Integer.MAX_VALUE + "")
    public void testTooBigConfiguredInlineSizeNotUsed() {
        List<Integer> valueTypes = Lists.newArrayList(
                Value.BOOLEAN, Value.SHORT, Value.DATE, Value.DATE, Value.DOUBLE, Value.FLOAT,
                Value.INT, Value.BYTE, Value.DECIMAL, Value.TIME, Value.TIMESTAMP, Value.UUID
        );

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();

        for (int i = 0; i < valueTypes.size(); i++) {
            Integer valueType = valueTypes.get(i);

            Column c = new Column("c" + i, new TypeInfo(valueType, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
            inlineIdxs.add(createHelper(c, false));
        }

        assertEquals(92, H2TreeIndexBase.computeInlineSize("idx", inlineIdxs, MAX_INLINE_SIZE, -1, log));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_MAX_INDEX_PAYLOAD_SIZE, value = Integer.MAX_VALUE + "")
    public void testTooBigConfiguredInlineSizeUsedBecauseOneOfFieldsIsNotFixed() {
        List<Integer> valueTypes = Lists.newArrayList(
                Value.BOOLEAN, Value.SHORT, Value.DATE, Value.DATE, Value.DOUBLE, Value.FLOAT,
                Value.INT, Value.BYTE, Value.DECIMAL, Value.TIME, Value.TIMESTAMP, Value.UUID,
                Value.STRING
        );

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();

        for (int i = 0; i < valueTypes.size(); i++) {
            Integer valueType = valueTypes.get(i);

            Column c = new Column("c" + i, new TypeInfo(valueType, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null));
            inlineIdxs.add(createHelper(c, false));
        }

        int cfgInlineSize = MAX_INLINE_SIZE - 1;
        assertEquals(cfgInlineSize, H2TreeIndexBase.computeInlineSize("idx", inlineIdxs, cfgInlineSize, -1, log));
    }

    private static InlineIndexColumn createHelper(Column col, boolean useOptimizedComp) {
        switch (col.getType().getValueType()) {
            case Value.BOOLEAN:
                return new BooleanInlineIndexColumn(col);

            case Value.BYTE:
                return new ByteInlineIndexColumn(col);

            case Value.SHORT:
                return new ShortInlineIndexColumn(col);

            case Value.INT:
                return new IntegerInlineIndexColumn(col);

            case Value.LONG:
                return new LongInlineIndexColumn(col);

            case Value.FLOAT:
                return new FloatInlineIndexColumn(col);

            case Value.DOUBLE:
                return new DoubleInlineIndexColumn(col);

            case Value.DATE:
                return new DateInlineIndexColumn(col);

            case Value.TIME:
                return new TimeInlineIndexColumn(col);

            case Value.TIMESTAMP:
                return new TimestampInlineIndexColumn(col);

            case Value.UUID:
                return new UuidInlineIndexColumn(col);

            case Value.STRING:
                return new StringInlineIndexColumn(col, useOptimizedComp);

            case Value.STRING_FIXED:
                return new FixedStringInlineIndexColumn(col, useOptimizedComp);

            case Value.STRING_IGNORECASE:
                return new StringIgnoreCaseInlineIndexColumn(col, useOptimizedComp);

            case Value.BYTES:
                return new BytesInlineIndexColumn(col, useOptimizedComp);

            case Value.JAVA_OBJECT:
                return new ObjectHashInlineIndexColumn(col);

            case Value.DECIMAL:
                return new DecimalInlineIndexColumn(col);
        }

        throw new IllegalStateException("Unknown value type=" + col.getType().getValueType());
    }

}
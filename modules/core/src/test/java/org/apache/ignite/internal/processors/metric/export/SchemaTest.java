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

package org.apache.ignite.internal.processors.metric.export;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class SchemaTest {
    private static final String ITEM_NAME_PREF = "item.name.";

    private static final int CNT = 4;

    private  static final String DISALLOWED = "disallowed";

    private  static final int ARR_EXPANDED_DELTA = 128;

    private static final int SCHEMA_OFF = 64;

    @Test
    public void testBuild() {
        Schema.Builder bldr = Schema.Builder.newInstance();

        int namesSize = 0;

        int dataSize = 0;

        for (byte i = 0; i < CNT; i++) {
            String name = ITEM_NAME_PREF + i;

            bldr.add(name, i);

            namesSize += name.getBytes(Schema.UTF_8).length;

            dataSize += Schema.TYPE_SIZE[i];
        }

        Schema schema = bldr.build();

        int exp = Byte.BYTES * CNT /* type fields */ + Integer.BYTES * CNT /* size fields */ + namesSize;

        assertEquals(exp, schema.length());

        assertEquals(dataSize, schema.dataSize());

        assertEquals(CNT, schema.items().size());
    }

    @Test(expected = IllegalStateException.class)
    public void testAddAfterBuild() {
        Schema.Builder bldr = Schema.Builder.newInstance();

        for (byte i = 0; i < CNT; i++)
            bldr.add(ITEM_NAME_PREF + i, i);

        bldr.build();

        bldr.add(DISALLOWED, (byte)0);
    }

    @Test(expected = IllegalStateException.class)
    public void testBuildAfterBuild() {
        Schema.Builder bldr = Schema.Builder.newInstance();

        for (byte i = 0; i < CNT; i++)
            bldr.add(ITEM_NAME_PREF + i, i);

        bldr.build();

        bldr.build();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSchemaItemsImmutable() {
        Schema.Builder bldr = Schema.Builder.newInstance();

        for (byte i = 0; i < CNT; i++)
            bldr.add(ITEM_NAME_PREF + i, i);

        Schema schema = bldr.build();

        schema.items().add(new SchemaItem(DISALLOWED, (byte)0));
    }

    @Test
    public void testSchemaToBytesFromBytes() {
        Schema.Builder bldr = Schema.Builder.newInstance();

        for (byte i = 0; i < CNT; i++)
            bldr.add(ITEM_NAME_PREF + i, i);

        Schema schema = bldr.build();

        byte[] arr = schema.toBytes();

        assertEquals(schema.length(), arr.length);

        Schema schema1 = Schema.fromBytes(arr);

        assertEquals(schema.length(), schema1.length());
        assertEquals(schema.dataSize(), schema1.dataSize());
        assertEquals(schema.items(), schema1.items());
    }

    @Test
    public void testSchemaToBytesFromBytesInPlace() {
        Schema.Builder bldr = Schema.Builder.newInstance();

        for (byte i = 0; i < CNT; i++)
            bldr.add(ITEM_NAME_PREF + i, i);

        Schema schema = bldr.build();

        byte[] arr = new byte[schema.length() + ARR_EXPANDED_DELTA];

        schema.toBytes(arr, SCHEMA_OFF);

        Schema schema1 = Schema.fromBytes(arr, SCHEMA_OFF, schema.length());

        assertEquals(schema.length(), schema1.length());
        assertEquals(schema.dataSize(), schema1.dataSize());
        assertEquals(schema.items(), schema1.items());
    }


    @Test(expected = IllegalArgumentException.class)
    public void testSchemaToBytesInPlaceBoundsViolated() {
        Schema.Builder bldr = Schema.Builder.newInstance();

        for (byte i = 0; i < CNT; i++)
            bldr.add(ITEM_NAME_PREF + i, i);

        Schema schema = bldr.build();

        byte[] arr = new byte[schema.length() + ARR_EXPANDED_DELTA];

        schema.toBytes(arr, ARR_EXPANDED_DELTA + SCHEMA_OFF / 2);
   }

    @Test(expected = IllegalArgumentException.class)
    public void testSchemaFromBytesInPlaceBoundsViolated() {
        Schema.Builder bldr = Schema.Builder.newInstance();

        for (byte i = 0; i < CNT; i++)
            bldr.add(ITEM_NAME_PREF + i, i);

        Schema schema = bldr.build();

        byte[] arr = new byte[schema.length() + ARR_EXPANDED_DELTA];

        schema.toBytes(arr, SCHEMA_OFF);

        Schema.fromBytes(arr, SCHEMA_OFF + ARR_EXPANDED_DELTA, schema.length());
    }
}
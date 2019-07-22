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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.ignite.internal.util.GridUnsafe;

import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;

/**
 * Schema defines data frame format and helps to reduce size of data during network communication.
 *
 * Schema is high-level description of the format and can be converted to compact byte array form.
 *
 * On high-level schema is list of {@link SchemaItem} instances. Each instance describes one particular metric.
 *
 * Schema provides way to compute data frame size before memory allocation.
 *
 * Compact schema representation:
 *
 * 4 bytes - key size (k)
 * k bytes - key bytes
 * 4 bytes - value type
 */
public class Schema {
    static final Charset UTF_8 = Charset.forName("UTF-8");
    static final int[] TYPE_SIZE = new int[] {1, Integer.BYTES, Long.BYTES, Double.BYTES};

    private final List<SchemaItem> items;
    private final int len;
    private final int dataSize;

    private Schema(List<SchemaItem> items, int len, int dataSize) {
        this.items = items;
        this.len = len;
        this.dataSize = dataSize;
    }

    public List<SchemaItem> items() {
        return Collections.unmodifiableList(items);
    }

    public static Schema fromBytes(byte[] arr, int off, int len) {
        if (len > arr.length - off) {
            throw new IllegalArgumentException("Schema can't be converted from byte array. " +
                    "Schema size is greater then size of array [len=" + len +
                    ", arr.length=" + arr.length + ", off=" + off + ']');
        }

        List<SchemaItem> items = new ArrayList<>();

        int dataSize = 0;

        for (int lim = off + len; off < lim;) {
            byte type = GridUnsafe.getByte(arr, BYTE_ARR_OFF + off);

            off += Byte.BYTES;

            int nameSize = GridUnsafe.getInt(arr, BYTE_ARR_OFF + off);

            off += Integer.BYTES;

            String name = new String(arr, off, nameSize, UTF_8);

            off += nameSize;

            SchemaItem item = new SchemaItem(name, type);

            items.add(item);

            dataSize += TYPE_SIZE[type];
        }

        return new Schema(items, len, dataSize);
    }

    /**
     * Converts byte representation of the scheme to the high-level representation.
     *
     * @param arr Byte array with compact schema representation.
     * @return High-level schema representation.
     */
    public static Schema fromBytes(byte[] arr) {
        return fromBytes(arr, 0, arr.length);
    }

    /**
     * Converts high-level schema representation to byte array.
     *
     * @return Compact schema representation.
     */
    public byte[] toBytes() {
        byte[] arr = new byte[len];

        toBytes(arr, 0);

        return arr;
    }

    public void toBytes(byte[] arr, int off) {
        if (len > arr.length - off) {
            throw new IllegalArgumentException("Schema can't be converted to byte array. " +
                    "Schema size is greater then size of array [len=" + len +
                    ", arr.length=" + arr.length + ", off=" + off + ']');
        }

        for (int i = 0; i < items.size(); i++) {
            SchemaItem item = items.get(i);

            GridUnsafe.putByte(arr, BYTE_ARR_OFF + off, item.type());

            off += Byte.BYTES;

            byte[] keyBytes = item.name().getBytes(UTF_8);

            GridUnsafe.putInt(arr, BYTE_ARR_OFF + off, keyBytes.length);

            off += Integer.BYTES;

            for (byte b : keyBytes) {
                GridUnsafe.putByte(arr, BYTE_ARR_OFF + off, b);

                off += Byte.BYTES;
            }
        }
    }

    private void add(String key, byte type) {
        items.add(new SchemaItem(key, type));
    }

    public int dataSize() {
        return dataSize;
    }

    public int length() {
        return len;
    }


    public static class Builder {
        private List<SchemaItem> items = new ArrayList<>();
        private int len;
        private int dataSize;

        public static Builder newInstance() {
            return new Builder();
        }

        public void add(String name, byte type) {
            if (items == null)
                throw new IllegalStateException("Builder can't be used twice.");

            SchemaItem item = new SchemaItem(name, type);

            items.add(item);

            dataSize += TYPE_SIZE[type];

            byte[] nameBytes = name.getBytes(UTF_8);

            len += Byte.BYTES + Integer.BYTES + nameBytes.length;
        }

        public Schema build() {
            if (items == null)
                throw new IllegalStateException("Builder can't be used twice.");

            Schema schema = new Schema(items, len, dataSize);

            items = null;

            return schema;
        }
    }
}

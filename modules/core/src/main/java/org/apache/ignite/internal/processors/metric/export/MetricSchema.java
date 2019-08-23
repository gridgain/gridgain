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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_INT_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;
import static org.apache.ignite.internal.util.GridUnsafe.getInt;
import static org.apache.ignite.internal.util.GridUnsafe.getShort;
import static org.apache.ignite.internal.util.GridUnsafe.putInt;
import static org.apache.ignite.internal.util.GridUnsafe.putShort;

/**
 * Schema index: describe all nested schemas. Format for each item:
 * (may this entity should be named as Schema while Schema should be renamed to MetricRegistrySchemaItem and MetricRegistrySchemaItem should be renamed to ValueDescriptor)
 *
 * 0 - short - schema index.
 * 2 - int - prefix size in bytes (k)
 * 6 - byte[k] - prefix bytes
 *
 * Items placed sequentially and can be iterated in order to provide convinient way for data fetching.
 */
public class MetricSchema {
    static final int REG_SCHEMA_IDX_SIZE = Short.BYTES;

    static final int SCHEMA_ITEM_CNT_SIZE = Short.BYTES;

    static final int PREF_BYTES_LEN_SIZE = Integer.BYTES;

    static final int REG_SCHEMA_CNT_SIZE = Short.BYTES;

    static final int REG_SCHEMA_OFF_SIZE = Integer.BYTES;

    private final List<MetricSchemaItem> items;

    private final List<MetricRegistrySchema> regSchemas;

    private final int len;

    private MetricSchema(List<MetricSchemaItem> items, List<MetricRegistrySchema> regSchemas, int len) {
        this.items = items;
        this.regSchemas = regSchemas;
        this.len = len;
    }

    public List<MetricSchemaItem> items() {
        return Collections.unmodifiableList(items);
    }

    public int length() {
        return len;
    }

    public MetricRegistrySchema registrySchema(short idx) {
        return regSchemas.get(idx);
    }

    public int registrySchemas() {
        return regSchemas.size();
    }


    public byte[] toBytes() {
        byte[] arr = new byte[len];

        toBytes(arr, 0);

        return arr;
    }

    public void toBytes(byte[] arr, int off) {
        if (len > arr.length - off) {
            throw new IllegalArgumentException("Schema can't be converted to byte array. " +
                    "Schema size is greater then size of array [estimatedLen=" + len +
                    ", arr.length=" + arr.length + ", off=" + off + ']');
        }

        putShort(arr, BYTE_ARR_OFF + off, (short)regSchemas.size());

        off += REG_SCHEMA_CNT_SIZE;

        int regSchemasOff = off;

        off += REG_SCHEMA_OFF_SIZE * regSchemas.size();

        putShort(arr, BYTE_ARR_OFF + off, (short)items.size());

        off += SCHEMA_ITEM_CNT_SIZE;

        for (int i = 0; i < items.size(); i++) {
            MetricSchemaItem item = items.get(i);

            putShort(arr, BYTE_ARR_OFF + off, item.index());

            off += Short.BYTES;

            byte[] prefBytes = item.prefix().getBytes(UTF_8);

            putInt(arr, BYTE_ARR_OFF + off, prefBytes.length);

            off += Integer.BYTES;

            copyMemory(prefBytes, BYTE_ARR_OFF, arr, BYTE_ARR_OFF + off, prefBytes.length);

            off += prefBytes.length;
        }

        for (int i = 0; i < regSchemas.size(); i++) {
            putInt(arr, BYTE_ARR_INT_OFF + regSchemasOff + REG_SCHEMA_OFF_SIZE * i, off - regSchemasOff);

            MetricRegistrySchema regSchema = regSchemas.get(i);

            regSchema.toBytes(arr, off);

            off += regSchema.length();
        }
    }

    public static MetricSchema fromBytes(byte[] arr, int off, int len) {
        if (len > arr.length - off) {
            throw new IllegalArgumentException("Schema can't be converted from byte array. " +
                    "Schema size is greater then size of array [len=" + len +
                    ", arr.length=" + arr.length + ", off=" + off + ']');
        }

        short regSchemasCnt = getShort(arr, BYTE_ARR_OFF + off);

        off += REG_SCHEMA_CNT_SIZE;

        int regSchemasOff = off;

        off += REG_SCHEMA_OFF_SIZE * regSchemasCnt;

        List<MetricRegistrySchema> regSchemas = new ArrayList<>(regSchemasCnt);

        int regSchemasSize = 0;

        for (int i = 0; i < regSchemasCnt; i++) {
            int regSchemaOff = getInt(arr, BYTE_ARR_OFF + regSchemasOff + REG_SCHEMA_OFF_SIZE * i) + regSchemasOff;

            int regSchemaLen = getInt(arr, BYTE_ARR_OFF + regSchemaOff);

            MetricRegistrySchema regSchema = MetricRegistrySchema.fromBytes(arr, regSchemaOff, regSchemaLen);

            regSchemas.add(regSchema);

            regSchemasSize += regSchema.length();
        }

        List<MetricSchemaItem> items = new ArrayList<>();

        off += SCHEMA_ITEM_CNT_SIZE;

        for (int lim = off + len - REG_SCHEMA_CNT_SIZE - REG_SCHEMA_OFF_SIZE * regSchemasCnt - SCHEMA_ITEM_CNT_SIZE - regSchemasSize; off < lim;) {
            short idx = getShort(arr, BYTE_ARR_OFF + off);

            off += REG_SCHEMA_IDX_SIZE;

            int prefSize = getInt(arr, BYTE_ARR_OFF + off);

            off += PREF_BYTES_LEN_SIZE;

            String pref = new String(arr, off, prefSize, UTF_8);

            off += prefSize;

            MetricSchemaItem item = new MetricSchemaItem(idx, pref);

            items.add(item);
        }

        return new MetricSchema(items, regSchemas, len);
    }

    /**
     * Converts byte representation of the scheme to the high-level representation.
     *
     * @param arr Byte array with compact schema representation.
     * @return High-level schema representation.
     */
    public static MetricSchema fromBytes(byte[] arr) {
        return fromBytes(arr, 0, arr.length);
    }


    public static class Builder {
        private List<MetricSchemaItem> items = new ArrayList<>();
        private List<MetricRegistrySchema> regSchemas = new ArrayList<>();
        private Map<String, Short> idxMap = new LinkedHashMap<>();
        private int len;

        public static MetricSchema.Builder newInstance() {
            return new MetricSchema.Builder();
        }

        public void add(String type, String pref, MetricRegistrySchema regSchema) {
            if (items == null)
                throw new IllegalStateException("Builder can't be used twice.");

            Short idx = idxMap.get(type);

            if (idx == null) {
                idx = (short)regSchemas.size();

                idxMap.put(type, idx);

                regSchemas.add(regSchema);

                len += regSchema.length();
            }

            MetricSchemaItem item = new MetricSchemaItem(idx, pref);

            items.add(item);

            byte[] prefBytes = pref.getBytes(UTF_8);

            len += REG_SCHEMA_IDX_SIZE + PREF_BYTES_LEN_SIZE + prefBytes.length;
        }

        public MetricSchema build() {
            if (items == null)
                throw new IllegalStateException("Builder can't be used twice.");

            len += SCHEMA_ITEM_CNT_SIZE + REG_SCHEMA_CNT_SIZE + REG_SCHEMA_OFF_SIZE * regSchemas.size();

            MetricSchema schema = new MetricSchema(items, regSchemas, len);

            items = null;

            return schema;
        }
    }

}

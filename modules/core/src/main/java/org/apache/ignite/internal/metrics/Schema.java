package org.apache.ignite.internal.metrics;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.ignite.internal.util.GridUnsafe;

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
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final int[] TYPE_SIZE = new int[] {4, 8, 4, 8};

    private final List<SchemaItem> items;
    private final int len;
    private final int dataSize;

    private Schema(List<SchemaItem> items, int len, int dataSize) {
        this.items = items;
        this.len = len;
        this.dataSize = dataSize;
    }

    /**
     * Converts byte representation of the scheme to the high-level representation.
     *
     * @param arr Byte array with compact schema representation.
     * @return High-level schema representation.
     */
    public static Schema fromBytes(byte[] arr) {
        List<SchemaItem> items = new ArrayList<>();

        int dataSize = 0;

        for (int off = 0; off < arr.length;) {
            int keySize = GridUnsafe.getInt(arr, GridUnsafe.BYTE_ARR_OFF + off);

            off += Integer.BYTES;

            String key = new String(arr, off, keySize, UTF_8);

            off += keySize;

            int type = GridUnsafe.getInt(arr, GridUnsafe.BYTE_ARR_OFF + off);

            off += Integer.BYTES;

            SchemaItem item = new SchemaItem(key, type);

            items.add(item);

            dataSize += TYPE_SIZE[type];
        }

        return new Schema(items, arr.length, dataSize);
    }

    /**
     * Converts high-level schema representation to byte array.
     *
     * @return Compact schema representation.
     */
    public byte[] toBytes() {
        byte[] arr = new byte[len];

        for (int i = 0, off = 0; i < items.size(); i++) {
            SchemaItem item = items.get(i);

            byte[] keyBytes = item.key.getBytes(UTF_8);

            GridUnsafe.putInt(arr, GridUnsafe.BYTE_ARR_OFF + off, keyBytes.length);

            off += Integer.BYTES;

            for (byte b : keyBytes)
                GridUnsafe.putByte(arr, GridUnsafe.BYTE_ARR_OFF + off, b);

            off += keyBytes.length;

            GridUnsafe.putInt(arr, GridUnsafe.BYTE_ARR_OFF + off, item.type);

            off += Integer.BYTES;
        }

        return arr;
    }

    private void add(String key, int type) {
        items.add(new SchemaItem(key, type));
    }

    public int dataSize() {
        return dataSize;
    }

    public int length() {
        return len;
    }

    public static class SchemaItem {
        private final String key;
        private final int type;

        public SchemaItem(String key, int type) {
            this.key = key;
            this.type = type;
        }
    }

    public static class SchemaBuilder {
        private List<SchemaItem> items = new ArrayList<>();
        private int len;
        private int dataSize;

        public static SchemaBuilder builder() {
            return new SchemaBuilder();
        }

        public void add(String key, int type) {
            if (items == null)
                throw new IllegalStateException("Builder can't be used twice.");

            SchemaItem item = new SchemaItem(key, type);

            items.add(item);

            dataSize += TYPE_SIZE[type];

            byte[] keyBytes = key.getBytes(UTF_8);

            len += Integer.BYTES + keyBytes.length + Integer.BYTES;
        }

        public Schema build() {
            if (items == null)
                throw new IllegalStateException("Builder can't be used twice.");

            items = null;

            return new Schema(items, len, dataSize);
        }
    }
}

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

public class VarIntByteBuffer {

    private byte[] arr;

    private int pos;

    public VarIntByteBuffer(byte[] arr) {
        this.arr = arr;
    }

    public void reset() {
        pos = 0;
    }

    public void putBoolean(boolean val) {
        ensureCapacity(1);

        arr[pos++] = (byte)(val ? 1 : 0);
    }

    public void putDouble(double val) {
        ensureCapacity(Double.BYTES);

        long v = Double.doubleToLongBits(val);

        arr[pos++] = (byte)(v >> 56);
        arr[pos++] = (byte)(v >> 48);
        arr[pos++] = (byte)(v >> 40);
        arr[pos++] = (byte)(v >> 32);
        arr[pos++] = (byte)(v >> 24);
        arr[pos++] = (byte)(v >> 16);
        arr[pos++] = (byte)(v >> 8);
        arr[pos++] = (byte)(v);
    }

    public void putVarInt(int val) {
        if ((val & (~0 << 7)) == 0) {
            ensureCapacity(1);

            arr[pos++] = (byte)val;
        }
        else if ((val & (~0 << 14)) == 0) {
            ensureCapacity(2);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 7);
        }
        else if ((val & (~0 << 21)) == 0) {
            ensureCapacity(3);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 14);
        }
        else if ((val & (~0 << 28)) == 0) {
            ensureCapacity(4);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 21);
        }
        else {
            ensureCapacity(5);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 21) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 28);
        }
    }

    public void putVarLong(long val) {
        if ((val & (~0L << 7)) == 0) {
            ensureCapacity(1);

            arr[pos++] = (byte)val;
        }
        else if ((val & (~0L << 14)) == 0) {
            ensureCapacity(2);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 7);
        }
        else if ((val & (~0L << 21)) == 0) {
            ensureCapacity(3);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 14);
        }
        else if ((val & (~0L << 28)) == 0) {
            ensureCapacity(4);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 21);
        }
        else if ((val & (~0L << 35)) == 0) {
            ensureCapacity(5);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 21) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 28);
        }
        else if ((val & (~0L << 42)) == 0) {
            ensureCapacity(6);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 21) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 28) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 35);
        }
        else if ((val & (~0L << 49)) == 0) {
            ensureCapacity(7);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 21) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 28) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 35) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 42);
        }
        else if ((val & (~0L << 56)) == 0) {
            ensureCapacity(8);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 21) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 28) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 35) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 42) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 49);
        }
        else if ((val & (~0L << 63)) == 0) {
            ensureCapacity(9);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 21) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 28) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 35) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 42) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 49) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 56);
        }
        else {
            ensureCapacity(10);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 21) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 28) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 35) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 42) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 49) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 56) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 63);
        }
    }

    public boolean getBoolean() {
        return arr[pos++] != 0;
    }

    public double getDouble() {
        long val = (((long)arr[pos++]) << 56) |
                (((long)arr[pos++]) & 0xFF) << 48 |
                (((long)arr[pos++]) & 0xFF) << 40 |
                (((long)arr[pos++]) & 0xFF) << 32 |
                (((long)arr[pos++]) & 0xFF) << 24 |
                (((long)arr[pos++]) & 0xFF) << 16 |
                (((long)arr[pos++]) & 0xFF) << 8 |
                (((long)arr[pos++]) & 0xFF);

        return Double.longBitsToDouble(val);
    }

    public int getVarInt() {
        int b, i = 0, res = 0;

        do {
            b = (arr[pos++] & 0xFF);

            res |= ((b & 0x7F) << (7 * i++));
        }
        while ((b & 0x80) != 0);

        return res;
    }

    public long getVarLong() {
        int i = 0;

        long b, res = 0;

        do {
            b = (arr[pos++] & 0xFF);

            res |= ((b & 0x7F) << (7 * i++));
        }
        while ((b & 0x80) != 0);

        return res;
    }

    public void toBytes(byte[] arr, int off) {
        System.arraycopy(this.arr, 0, arr, off, pos);
    }

    public void position(int pos) {
        this.pos = pos;
    }

    public int position() {
        return pos;
    }

    private void ensureCapacity(int len) {
        if (arr.length - pos < len) {
            byte[] tmp = new byte[Math.max(arr.length * 2, arr.length + len)];

            System.arraycopy(arr, 0, tmp, 0, pos);

            arr = tmp;
        }
    }
}

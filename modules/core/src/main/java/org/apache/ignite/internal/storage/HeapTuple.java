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

import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.binary.BinaryPrimitives;

/**
 *
 */
public class HeapTuple extends Tuple {
    /** */
    private final byte[] arr;

    /**
     * @param arr Array representation of the tuple.
     */
    public HeapTuple(byte[] arr) {
        this.arr = arr;
    }

    /** {@inheritDoc} */
    @Override protected String readString(int off, int len) {
        return new String(arr, off, len, StandardCharsets.UTF_8);
    }

    /** {@inheritDoc} */
    @Override protected long readVarlong(int off) {
        return BinaryPrimitives.readVarlong(arr, off);
    }

    /** {@inheritDoc} */
    @Override protected long readLong(int off) {
        return BinaryPrimitives.readLong(arr, off);
    }

    /** {@inheritDoc} */
    @Override protected int readInteger(int off) {
        return BinaryPrimitives.readInt(arr, off);
    }

    /** {@inheritDoc} */
    @Override protected int readShort(int off) {
        return BinaryPrimitives.readShort(arr, off) & 0xFFFF;
    }

    /** {@inheritDoc} */
    @Override protected int readByte(int off) {
        return arr[off] & 0xFF;
    }
}

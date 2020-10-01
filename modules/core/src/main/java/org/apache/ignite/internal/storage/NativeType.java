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

/**
 *
 */
public class NativeType implements Comparable<NativeType> {
    public static final NativeType INTEGER = new NativeType("integer", 4);
    public static final NativeType LONG = new NativeType("long", 8);
    public static final NativeType DATE = new NativeType("date", 3);
    public static final NativeType TIME = new NativeType("time", 5);
    public static final NativeType DATETIME = new NativeType("datetime", 8);
    public static final NativeType INSTANT = new NativeType("instant", 8);
    public static final NativeType UUID = new NativeType("uuid", 16);
    public static final NativeType STRING = new NativeType("string");
    public static final NativeType VARLONG = new NativeType("varlong");

    private final int size;

    private final String desc;

    private NativeType(String desc) {
        this(desc, -1);
    }

    private NativeType(String desc, int size) {
        this.desc = desc;
        this.size = size;
    }

    public int size() {
        return size;
    }

    public boolean fixedSize() {
        return size > 0;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        NativeType that = (NativeType)o;

        return size == that.size &&
            desc.equals(that.desc);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return desc.hashCode() + 31 * size;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(NativeType o) {
        // Fixed-sized types go first.
        if (size < 0 && o.size > 0)
                return 1;

        if (size > 0 && o.size < 0)
            return -1;

        // Either size is -1 for both, or positive for both. Compare sizes, then description.
        int cmp = Integer.compare(size, o.size);

        if (cmp != 0)
            return cmp;

        return desc.compareTo(o.desc);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "NativeType [desc=" + desc + ", size=" + (fixedSize() ? size : "varlen") + ']';
    }
}

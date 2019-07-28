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

import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;

public enum MetricType {
    BOOLEAN((byte)0, 1, BooleanMetric.class),

    INT((byte)1, Integer.BYTES, IntMetric.class),

    LONG((byte)2, Long.BYTES, LongMetric.class),

    DOUBLE((byte)3, Double.BYTES, DoubleMetric.class);

    private static final MetricType[] typeIdx = new MetricType[MetricType.values().length];

    private static final Map<Class, MetricType> classIdx = new HashMap<>();

    private final int size;

    private final byte type;

    private final Class cls;

    MetricType(byte type, int size, Class cls) {
        this.type = type;
        this.size = size;
        this.cls = cls;
    }

    public int size() {
        return size;
    }

    public byte type() {
        return type;
    }

    public Class clazz() {
        return cls;
    }

    public static MetricType findByType(byte type) {
        MetricType res = typeIdx[type];

        if (res == null) {
            for (MetricType m : MetricType.values()) {
                if (m.type() == type) {
                    res = m;

                    typeIdx[type] = m;
                }
            }
        }

        return res;
    }

    public static MetricType findByClass(Class cls) {
        MetricType res = classIdx.get(cls);

        if (res == null) {
            for (MetricType m : MetricType.values()) {
                if (m.clazz().isAssignableFrom(cls)) {
                    res = m;

                    classIdx.put(cls, m);
                }
            }
        }

        assert res != null;

        return res;
    }
}

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

import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public enum MetricType {
    BOOLEAN((byte)0, BooleanMetric.class),

    INT((byte)1, IntMetric.class),

    LONG((byte)2, LongMetric.class),

    DOUBLE((byte)3, DoubleMetric.class),

    HIT_RATE((byte)4, HitRateMetric.class),

    HISTOGRAM((byte)5, HistogramMetric.class);

    private static final MetricType[] typeIdx = new MetricType[MetricType.values().length];

    private static final Map<Class, MetricType> classIdx = new HashMap<>();

    private final byte type;

    private final Class<?> cls;

    MetricType(byte type, Class cls) {
        this.type = type;
        this.cls = cls;
    }

    public byte type() {
        return type;
    }

    public Class clazz() {
        return cls;
    }

    @NotNull public static MetricType findByType(byte type) {
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

    @Nullable public static MetricType findByClass(Class cls) {
        // HitRateMetric implements LongMetric interface so we need handle this case in the specific manner.
        if (cls.equals(HitRateMetric.class))
            return HIT_RATE;

        // HistogramMetric implements ObjectMetric interface so we need handle this case in the specific manner.
        if (cls.equals(HistogramMetric.class))
            return HISTOGRAM;

        MetricType res = classIdx.get(cls);

        if (res == null) {
            for (MetricType m : MetricType.values()) {
                if (m.cls.isAssignableFrom(cls)) {
                    res = m;

                    classIdx.put(cls, m);
                }
            }
        }

        return res;
    }
}

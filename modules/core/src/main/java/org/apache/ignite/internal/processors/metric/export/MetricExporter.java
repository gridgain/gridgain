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
import java.util.Objects;
import java.util.UUID;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;
import static org.apache.ignite.internal.util.GridUnsafe.putBoolean;
import static org.apache.ignite.internal.util.GridUnsafe.putDouble;
import static org.apache.ignite.internal.util.GridUnsafe.putInt;
import static org.apache.ignite.internal.util.GridUnsafe.putLong;

/**
 * header
 * schema
 * reg schemas idx
 * reg schemas
 * data
 */

public class MetricExporter {
    private final Map<Integer, IgniteBiTuple<MetricSchema, byte[]>> schemas = new HashMap<>();
    private final Map<String, IgniteBiTuple<MetricRegistrySchema, byte[]>> registrySchemas = new HashMap<>();

    public MetricResponse export(GridKernalContext ctx) {
        UUID clusterId = UUID.randomUUID(); //TODO: get real cluster ID

        String userTag = "test_user_tag"; //TODO: get real user tag

        Map<String, MetricRegistry> metrics = ctx.metric().registries();

        String consistentId = (String)ctx.config().getConsistentId();

        assert consistentId != null : "consistent ID is null";

        return metricMessage(clusterId, userTag, consistentId, metrics);
    }

    @NotNull
    MetricResponse metricMessage(UUID clusterId, String userTag, String consistentId, Map<String, MetricRegistry> metrics) {
        int ver = schemaVersion(metrics);

        long ts = System.currentTimeMillis();

        IgniteBiTuple<MetricSchema, byte[]> tup = schemas.get(ver);

        if (tup == null) {
            //TODO: remove obsolete versions
            MetricSchema schema = generateSchema(metrics);

            schemas.put(ver, tup = new IgniteBiTuple<>(schema, schema.toBytes()));
        }

        MetricSchema schema = tup.get1();

        byte[] schemaBytes = tup.get2();

        return new MetricResponse(
                ver,
                ts,
                clusterId,
                userTag,
                consistentId,
                schema.length(),
                schema.dataSize(),
                (arr, off) -> writeSchema(schemaBytes, arr, off),
                (arr, off) -> writeData(arr, off, metrics)
        );
    }

    private MetricSchema generateSchema(Map<String, MetricRegistry> metrics) {
        MetricSchema.Builder bldr = MetricSchema.Builder.newInstance();

        for (MetricRegistry reg : metrics.values()) {
            MetricRegistrySchema regSchema = generateOrGetRegistrySchema(reg);

            bldr.add(MetricRegistry.class, reg.name(), regSchema);
        }

        return bldr.build();
    }

    private MetricRegistrySchema generateOrGetRegistrySchema(MetricRegistry reg) {
        IgniteBiTuple<MetricRegistrySchema, byte[]> tup = registrySchemas.computeIfAbsent(
                reg.type(),
                type -> {
                    MetricRegistrySchema schema = generateMetricRegistrySchema(reg);

                    return new IgniteBiTuple<>(schema, schema.toBytes());
                }
        );

        return tup.get1();
    }

    private MetricRegistrySchema generateMetricRegistrySchema(MetricRegistry reg) {
        MetricRegistrySchema.Builder bldr = MetricRegistrySchema.Builder.newInstance();

        for (Map.Entry<String, Metric> e : reg.metrics().entrySet()) {
            String name = e.getKey();

            Metric m = e.getValue();

            MetricType metricType = MetricType.findByClass(m.getClass());

            if (metricType != null)
                bldr.add(name, metricType);
        }

        return bldr.build();
    }

    private int schemaVersion(Map<String, MetricRegistry> metrics) {
        //TODO: schemaVer --> ver (schemaVer + node.ver + topVer)
        return Objects.hash(metrics.keySet());
    }

    private static void writeSchema(byte[] schemaBytes0, byte[] arr, Integer off) {
        copyMemory(schemaBytes0, BYTE_ARR_OFF, arr, BYTE_ARR_OFF + off, schemaBytes0.length);
    }

    private static void writeData(byte[] arr, int dataFrameOff, Map<String, MetricRegistry> metrics) {
        int off = dataFrameOff;

        for (Map.Entry<String, MetricRegistry> r : metrics.entrySet()) {
            for (Map.Entry<String, Metric> e : r.getValue().metrics().entrySet()) {
                Metric m = e.getValue();

                if (m instanceof BooleanMetric) {
                    putBoolean(arr, BYTE_ARR_OFF + off, ((BooleanMetric)m).value());

                    off += 1;
                }
                else if (m instanceof IntMetric) {
                    putInt(arr, BYTE_ARR_OFF + off, ((IntMetric)m).value());

                    off += Integer.BYTES;
                }
                else if (m instanceof LongMetric) {
                    putLong(arr, BYTE_ARR_OFF + off, ((LongMetric)m).value());

                    off += Long.BYTES;
                }
                else if (m instanceof DoubleMetric) {
                    putDouble(arr, BYTE_ARR_OFF + off, ((DoubleMetric)m).value());

                    off += Double.BYTES;
                }
            }
        }
    }
}

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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.GridTopic.TOPIC_METRICS;
import static org.apache.ignite.internal.processors.metric.export.MetricType.BOOLEAN;
import static org.apache.ignite.internal.processors.metric.export.MetricType.DOUBLE;
import static org.apache.ignite.internal.processors.metric.export.MetricType.HISTOGRAM;
import static org.apache.ignite.internal.processors.metric.export.MetricType.HIT_RATE;
import static org.apache.ignite.internal.processors.metric.export.MetricType.INT;
import static org.apache.ignite.internal.processors.metric.export.MetricType.LONG;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;

/**
 * header
 * schema
 * reg schemas idx
 * reg schemas
 * data
 */

public class MetricExporter extends GridProcessorAdapter {
    private final Map<Integer, IgniteBiTuple<MetricSchema, byte[]>> schemas = new HashMap<>();
    private final Map<String, IgniteBiTuple<MetricRegistrySchema, byte[]>> registrySchemas = new HashMap<>();

    /**
     * @param ctx Kernal context.
     */
    public MetricExporter(GridKernalContext ctx) {
        super(ctx);

        ctx.io().addMessageListener(TOPIC_METRICS, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (msg instanceof MetricRequest) {
                    int schemaVer = ((MetricRequest) msg).schemaVersion();

                    if (schemaVer == -1) {
                        MetricResponse res = export();

                        try {
                            ctx.io().sendToGridTopic(nodeId, TOPIC_METRICS, res, plc);
                        }
                        catch (IgniteCheckedException e) {
                            log.error("Error during sending message [topic" + TOPIC_METRICS +
                                    ", dstNodeId=" + nodeId + ", msg=" + msg + ']');
                        }
                    }
                }
            }
        });

    }

    public MetricResponse export() {
        IgniteCluster cluster = ctx.cluster().get();

        UUID clusterId = cluster.id();

        assert clusterId != null : "ClusterId is null.";

        String tag = cluster.tag();

        String consistentId = (String)ctx.discovery().localNode().consistentId();

        assert consistentId != null : "ConsistentId is null.";

        Map<String, MetricRegistry> metrics = ctx.metric().registries();

        return metricMessage(clusterId, tag, consistentId, metrics);
    }

    @NotNull
    MetricResponse metricMessage(UUID clusterId, String userTag, String consistentId, Map<String, MetricRegistry> metrics) {
        int ver = 0; //schemaVersion(metrics);

        long ts = System.currentTimeMillis();

        //IgniteBiTuple<MetricSchema, byte[]> tup = schemas.get(ver);

        IgniteBiTuple<MetricSchema, byte[]> tup = null;

        if (tup == null) {
            //TODO: remove obsolete versions

            long startTs = System.currentTimeMillis();

            MetricSchema schema = generateSchema(metrics);

            System.out.println("Generate schema: " + (System.currentTimeMillis() - startTs) + " ms");

            schemas.put(ver, tup = new IgniteBiTuple<>(schema, schema.toBytes()));
        }

        MetricSchema schema = tup.get1();

        byte[] schemaBytes = tup.get2();

        long startTs = System.currentTimeMillis();

        VarIntByteBuffer data = generateData(metrics);

        MetricResponse metricResponse = new MetricResponse(
                ver,
                ts,
                clusterId,
                userTag,
                consistentId,
                schema.length(),
                data.position(),
                (arr, off) -> writeSchema(schemaBytes, arr, off),
                (arr, off) -> writeData(arr, off, data)
        );

        System.out.println("Message generation: " + (System.currentTimeMillis() - startTs) + " ms");

        return metricResponse;
    }

    private MetricSchema generateSchema(Map<String, MetricRegistry> metrics) {
        MetricSchema.Builder bldr = MetricSchema.Builder.newInstance();

        for (MetricRegistry reg : metrics.values()) {
            MetricRegistrySchema regSchema = generateOrGetRegistrySchema(reg);

            bldr.add(reg.type(), reg.name(), regSchema);
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

    private static void writeData(byte[] arr, int dataFrameOff, VarIntByteBuffer data) {
        data.toBytes(arr, dataFrameOff);
    }

    private static VarIntByteBuffer generateData(Map<String, MetricRegistry> metrics) {
        VarIntByteBuffer buf = new VarIntByteBuffer(new byte[1024]);

        for (Map.Entry<String, MetricRegistry> r : metrics.entrySet()) {
            for (Map.Entry<String, Metric> e : r.getValue().metrics().entrySet()) {
                Metric m = e.getValue();

                MetricType type = MetricType.findByClass(m.getClass());

                // Unsupported type.
                if (type == null)
                    continue;

                // Most popular metric types are first.
                if (type == LONG)
                    buf.putVarLong(((LongMetric)m).value());
                else if (type == INT)
                    buf.putVarInt(((IntMetric)m).value());
                else if (type == HIT_RATE) {
                    HitRateMetric metric = (HitRateMetric)m;

                    buf.putVarLong(metric.rateTimeInterval());

                    buf.putVarLong(metric.value());
                }
                else if (type == HISTOGRAM) {
                    HistogramMetric metric = (HistogramMetric)m;

                    long[] bounds = metric.bounds();

                    long[] vals = metric.value();

                    buf.putVarInt(bounds.length);

                    // Pais
                    for (int i = 0; i < bounds.length; i++) {
                        buf.putVarLong(bounds[i]);

                        buf.putVarLong(vals[i]);
                    }

                    // Infinity value.
                    buf.putVarLong(vals[vals.length - 1]);
                }
                else if (type == DOUBLE)
                    buf.putDouble(((DoubleMetric) m).value());
                else if (type == BOOLEAN)
                    buf.putBoolean(((BooleanMetric)m).value());
                else
                    throw new IllegalStateException("Unknown metric type [metric=" + m + ", type=" + type + ']');
            }
        }

        return buf;
    }

}

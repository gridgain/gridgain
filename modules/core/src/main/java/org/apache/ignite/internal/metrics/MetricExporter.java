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

package org.apache.ignite.internal.metrics;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.GridUnsafe;

public class MetricExporter {
    private final Map<Integer, Schema> schemas = new HashMap<>();

    public MetricMessage export(Ignite ignite) {
        Map<String, MetricRegistry> metrics = ((IgniteEx)ignite).context().metrics().metrics();

        int hash = metrics.hashCode();

        Schema schema = schemas.get(hash);

        int dataFrameSize = 0;

        boolean generateSchema = schema == null;

        if (generateSchema) {
            Schema.SchemaBuilder bldr = Schema.SchemaBuilder.builder();

            for (Map.Entry<String, MetricRegistry> m : metrics.entrySet()) {
                for (Map.Entry<String, Gauge> e : m.getValue().metrics().entrySet()) {
                    String key = e.getKey();

                    int type;

                    Gauge g = e.getValue();

                    if (g instanceof IntGauge)
                        type = 0;
                    else if (g instanceof LongGauge)
                        type = 1;
                    else if (g instanceof FloatGauge)
                        type = 2;
                    else if (g instanceof DoubleGauge)
                        type = 3;
                    else {
                        // TODO: log error about incorrect type

                        continue;
                    }

                    bldr.add(key, type);
                }
            }

            schema = bldr.build();

            schemas.put(hash, schema);
        }

        //TODO:

        int schemaFrameSize = schema.length();

        dataFrameSize = schema.dataSize();

        byte[] dataFrame = new byte[dataFrameSize];

        byte[] schemaFrame = null;

        if (generateSchema)
            schemaFrame = new byte[schemaFrameSize];

        int dataFrameOff = 0;
        int schemaFrameOff = 0;

        for (Map.Entry<String, MetricRegistry> m : metrics.entrySet()) {
            for (Map.Entry<String, Gauge> e : m.getValue().metrics().entrySet()) {
                String key = e.getKey();

                Gauge g = e.getValue();

                if (generateSchema) {
                    byte[] keyBytes;

                    try {
                        keyBytes = key.getBytes("UTF-8");
                    } catch (UnsupportedEncodingException ex) {
                        throw new IgniteException(ex);
                    }

                    GridUnsafe.putInt(schemaFrame, GridUnsafe.BYTE_ARR_OFF + schemaFrameOff, keyBytes.length);
                    schemaFrameOff += 4;

                    for (byte b : keyBytes) {
                        GridUnsafe.putByte(schemaFrame, GridUnsafe.BYTE_ARR_OFF + schemaFrameOff, b);

                        schemaFrameOff++;
                    }
                }

                if (g instanceof LongGauge) {
                    if (generateSchema) {
                        GridUnsafe.putInt(schemaFrame, GridUnsafe.BYTE_ARR_OFF + schemaFrameOff, 1);

                        schemaFrameOff += 4;
                    }

                    GridUnsafe.putLong(dataFrame, GridUnsafe.BYTE_ARR_OFF + dataFrameOff, ((LongGauge)g).value());

                    dataFrameOff += 8;
                }
            }
        }

        return new MetricMessage(schemaFrame, dataFrame);
    }
}

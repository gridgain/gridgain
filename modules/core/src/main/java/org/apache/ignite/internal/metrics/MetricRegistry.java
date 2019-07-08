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

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;


public class MetricRegistry {
    private Integer hashCode;

    private final Map<String, Gauge> metrics = new TreeMap<>();

    private final String name;

    public MetricRegistry() {
        this(null);
    }

    public MetricRegistry(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    public void add(String key, Gauge m) {
        if (name != null)
            key = name + '.' + key;

        Gauge old = metrics.putIfAbsent(key, m);

        assert old == null : "Metric " + key + " is already registered.";
    }

    Map<String, Gauge> metrics() {
        return Collections.unmodifiableMap(metrics);
    }

    @Override public int hashCode() {
        if (hashCode == null) {
            int h = 0;

            for (String s : metrics.keySet())
                h += s.hashCode();

            hashCode = h;

            return h;
        }

        return hashCode;
    }
}

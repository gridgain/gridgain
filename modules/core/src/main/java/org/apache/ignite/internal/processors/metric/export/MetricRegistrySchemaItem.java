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

import java.util.Objects;

import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.NotNull;

public class MetricRegistrySchemaItem {
    private final String name;
    private final MetricType metricType;

    public MetricRegistrySchemaItem(@NotNull String name, MetricType metricType) {
        this.name = name;
        this.metricType = metricType;
    }

    public String name() {
        return name;
    }

    public MetricType metricType() {
        return metricType;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        MetricRegistrySchemaItem that = (MetricRegistrySchemaItem) o;

        return name.equals(that.name) && metricType == that.metricType;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name, metricType);
    }
}

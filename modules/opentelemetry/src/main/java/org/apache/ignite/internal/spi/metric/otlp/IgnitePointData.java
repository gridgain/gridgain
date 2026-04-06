/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.spi.metric.otlp;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.metrics.data.PointData;

/**
 * Base class for a point in the metric data model.
 */
abstract class IgnitePointData implements PointData {
    /** {@inheritDoc} */
    @Override public long getStartEpochNanos() {
        return Clock.getDefault().now();
    }

    /** {@inheritDoc} */
    @Override public long getEpochNanos() {
        return Clock.getDefault().now();
    }

    /** {@inheritDoc} */
    @Override public Attributes getAttributes() {
        return Attributes.empty();
    }
}

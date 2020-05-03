/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.tracing.configuration;

import java.util.Collections;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

/**
 * Noop tracing configuration.
 * To be used mainly with {@link org.apache.ignite.internal.processors.tracing.NoopTracing}.
 */
public final class NoopTracingConfiguration implements TracingConfiguration {
    /** */
    public static final NoopTracingConfiguration INSTANCE = new NoopTracingConfiguration();

    /** {@inheritDoc} */
    @Override public void addConfiguration(@NotNull TracingConfigurationCoordinates coordinates,
        @NotNull TracingConfigurationParameters parameters) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public @NotNull TracingConfigurationParameters retrieveConfiguration(
        @NotNull TracingConfigurationCoordinates coordinates) {
        return NoopTracingConfigurationParameters.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Map<TracingConfigurationCoordinates, TracingConfigurationParameters> retrieveConfigurations() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public void restoreDefaultConfiguration(@NotNull TracingConfigurationCoordinates coordinates) {
        // No-op.
    }
}

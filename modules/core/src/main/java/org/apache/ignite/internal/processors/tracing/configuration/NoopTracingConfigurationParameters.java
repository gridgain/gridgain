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
import java.util.Set;
import org.apache.ignite.internal.processors.tracing.Scope;
import org.jetbrains.annotations.NotNull;

/**
 * Noop tracing configuration parameters. To be used mainly within {@link NoopTracingConfiguration}.
 */
public final class NoopTracingConfigurationParameters extends TracingConfigurationParameters {
    /** */
    public static final NoopTracingConfigurationParameters INSTANCE = new NoopTracingConfigurationParameters();

    /** {@inheritDoc} */
    @Override public @NotNull TracingConfigurationParameters withSamplingRate(double samplingRate) {
        throw new UnsupportedOperationException("Operation not supported.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull TracingConfigurationParameters withSupportedScopes(Set<Scope> supportedScopes) {
        throw new UnsupportedOperationException("Operation not supported.");
    }

    /** {@inheritDoc} */
    @Override public double samplingRate() {
        return 0d;
    }

    /** {@inheritDoc} */
    @Override public @NotNull Set<Scope> supportedScopes() {
        return Collections.emptySet();
    }
}

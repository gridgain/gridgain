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

import org.apache.ignite.internal.processors.tracing.Scope;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Specifies to which traces, specific configuration will be applied. In other words it's a sort of tracing
 * configuration locators.
 */
public class TracingConfigurationCoordinates {
    /**
     * Specifies the {@link Scope} of a trace's root span to which some specific tracing configuration will be applied.
     * It's a mandatory attribute.
     */
    private final Scope scope;

    /**
     * Specifies the label of a traced operation. It's an optional attribute.
     */
    private String lb;

    /**
     * Constructor.
     *
     * @param scope Specifies the {@link Scope} of a trace's root span to which some specific tracing configuration will
     * be applied.
     */
    public TracingConfigurationCoordinates(@NotNull Scope scope) {
        this.scope = scope;
    }

    /**
     * Builder method that allows to set optional label attribute.
     *
     * @param lb Label of traced operation. It's an optional attribute.
     * @return Current {@code TracingConfigurationCoordinates} instance.
     */
    public @NotNull TracingConfigurationCoordinates withLabel(@Nullable String lb) {
        this.lb = lb;

        return this;
    }

    /**
     * @return {@link Scope} of a trace's root span to which some specific tracing configuration will be applied.
     */
    @NotNull public Scope scope() {
        return scope;
    }

    /**
     * @return Label of a traced operation, to which some specific tracing configuration will be applied.
     */
    @Nullable public String label() {
        return lb;
    }
}

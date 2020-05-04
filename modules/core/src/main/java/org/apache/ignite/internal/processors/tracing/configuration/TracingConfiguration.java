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
import org.apache.ignite.internal.processors.tracing.Scope;
import org.jetbrains.annotations.NotNull;

/**
 * Allows to configure tracing, read the configuration and restore it to the defaults.
 */
public interface TracingConfiguration {
    // TODO: 04.05.20 After implementing GG-21041 and GG-21042 default TX config will include Scope.CACHE_GET, etc.
    /** Default transaction tracing configuration. */
    static final TracingConfigurationParameters DEFAULT_TX_CONFIGURATION =
        new TracingConfigurationParameters.Builder().
            withSamplingRate(0d).
            withSupportedScopes(Collections.emptySet()).
            build();

    /** Default exchange configuration. */
    static final TracingConfigurationParameters DEFAULT_EXCHANGE_CONFIGURATION =
        new TracingConfigurationParameters.Builder().
            withSamplingRate(0d).
            withSupportedScopes(Collections.emptySet()).
            build();

    /** Default discovery configuration. */
    static final TracingConfigurationParameters DEFAULT_DISCOVERY_CONFIGURATION =
        new TracingConfigurationParameters.Builder().
            withSamplingRate(0d).
            withSupportedScopes(Collections.emptySet()).
            build();

    /** Default communication configuration. */
    static final TracingConfigurationParameters DEFAULT_COMMUNICATION_CONFIGURATION =
        new TracingConfigurationParameters.Builder().
            withSamplingRate(0d).
            withSupportedScopes(Collections.emptySet()).
            build();

    /** Default noop configuration. */
    static final TracingConfigurationParameters NOOP_CONFIGURATION =
        new TracingConfigurationParameters.Builder().
            withSamplingRate(0d).
            withSupportedScopes(Collections.emptySet()).
            build();

    /**
     * Configure tracing parameters such as sampling rate for the specific tracing coordinates such as scope and label.
     *
     * @param coordinates {@link TracingConfigurationCoordinates} Specific set of locators like {@link Scope} and label
     *  that defines subset of traces and/or spans that'll use given configuration.
     * @param parameters{@link TracingConfigurationParameters} e.g. sampling rate, set of supported scopes etc.
     * @return {@code true} if new configuration was successfully added, {@code false} otherwise.
     */
    boolean addConfiguration(@NotNull TracingConfigurationCoordinates coordinates,
        @NotNull TracingConfigurationParameters parameters);

    /**
     * Retrieve the most specific tracing parameters for the specified tracing coordinates (scope and label).
     * The most specific means
     *  that if there's no configuration for the whole set of {@link TracingConfigurationCoordinates} attributes then
     *  {@link Scope} based configuration will be used.
     *  If scope based configuration also not specified then default one will be used.
     *
     * @param coordinates {@link TracingConfigurationCoordinates} Specific set of locators like {@link Scope} and label
     *  that defines subset of traces and/or spans that'll use given configuration.
     * @return {@link TracingConfigurationParameters} instance.
     */
    default @NotNull TracingConfigurationParameters retrieveConfiguration(
        @NotNull TracingConfigurationCoordinates coordinates)
    {
        switch (coordinates.scope()) {
            case TX: {
                return DEFAULT_TX_CONFIGURATION;
            }

            case EXCHANGE: {
                return DEFAULT_EXCHANGE_CONFIGURATION;
            }

            case DISCOVERY: {
                return DEFAULT_DISCOVERY_CONFIGURATION;
            }

            case COMMUNICATION: {
                return DEFAULT_COMMUNICATION_CONFIGURATION;
            }

            default: {
                return NOOP_CONFIGURATION;
            }
        }
    }

    /**
     * List all pairs of tracing configuration coordinates and tracing configuration parameters.
     *
     * @return The whole set of tracing configuration.
     */
    @NotNull Map<TracingConfigurationCoordinates, TracingConfigurationParameters> retrieveConfigurations();

    /**
     * Restore the tracing parameters for the specified tracing coordinates to the default.
     * In other words, removes any custom tracing configuration fot the specific {@link TracingConfigurationCoordinates}
     * @param coordinates {@link TracingConfigurationCoordinates} Specific set of locators like {@link Scope} and label
     *  that defines subset of traces and/or spans that'll use given configuration.
     * @return {@code true} if configuration was successfully restored to default, {@code false} otherwise.
     */
    boolean restoreDefaultConfiguration(@NotNull TracingConfigurationCoordinates coordinates);
}

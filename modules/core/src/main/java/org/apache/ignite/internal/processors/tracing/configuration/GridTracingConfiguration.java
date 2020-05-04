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
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.processors.tracing.Scope;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.jetbrains.annotations.NotNull;

/**
 * Tracing configuration implementation that uses distributed meta storage in order to store tracing configuration.
 */
public class GridTracingConfiguration implements TracingConfiguration {
    /** */
    private static final String TRACING_CONFIGURATION_DISTRIBUTED_METASTORE_KEY_PREFIX =
        DistributedMetaStorageImpl.IGNITE_INTERNAL_KEY_PREFIX + "tr.config.";

    /** Kernal context. */
    @GridToStringExclude
    protected final GridKernalContext ctx;

    /** Grid logger. */
    @GridToStringExclude
    protected final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public GridTracingConfiguration(@NotNull GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public boolean addConfiguration(
        @NotNull TracingConfigurationCoordinates coordinates,
        @NotNull TracingConfigurationParameters parameters)
    {
        DistributedMetaStorage metaStore;

        try {
            metaStore = ctx.distributedMetastorage();
        }
        catch (Exception e) {
            log.warning("Failed to save tracing configuration to meta storage. Meta storage is not available");

            return false;
        }

        if (metaStore == null) {
            log.warning("Failed to save tracing configuration to meta storage. Meta storage is not available");

            return false;
        }

        String scopeBasedKey = TRACING_CONFIGURATION_DISTRIBUTED_METASTORE_KEY_PREFIX + coordinates.scope().name();

        boolean configurationSuccessfullyUpdated = false;

        try {
            while (!configurationSuccessfullyUpdated) {
                HashMap<String, TracingConfigurationParameters> existingScopeBasedTracingConfiguration =
                    ctx.distributedMetastorage().read(scopeBasedKey);

                HashMap<String, TracingConfigurationParameters> updatedScopeBasedTracingConfiguration =
                    existingScopeBasedTracingConfiguration != null ?
                        new HashMap<>(existingScopeBasedTracingConfiguration) : new
                        HashMap<>();

                updatedScopeBasedTracingConfiguration.put(coordinates.label(), parameters);

                configurationSuccessfullyUpdated = ctx.distributedMetastorage().compareAndSet(
                    scopeBasedKey,
                    existingScopeBasedTracingConfiguration,
                    updatedScopeBasedTracingConfiguration);
            }
        }
        catch (IgniteCheckedException e) {
            log.warning("Failed to save tracing configuration to meta storage.", e);

            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public @NotNull TracingConfigurationParameters retrieveConfiguration(
        @NotNull TracingConfigurationCoordinates coordinates) {
        DistributedMetaStorage metaStore;

        try {
            metaStore = ctx.distributedMetastorage();
        }
        catch (Exception e) {
            LT.warn(log, "Failed to retrieve tracing configuration — meta storage is not available." +
                " Default value will be used.");

            // If metastorage in not available — use scope specific default tracing configuration.
            return TracingConfiguration.super.retrieveConfiguration(coordinates);
        }

        if (metaStore == null) {
            LT.warn(log, "Failed to retrieve tracing configuration — meta storage is not available." +
                " Default value will be used.");

            // If metastorage in not available — use scope specific default tracing configuration.
            return TracingConfiguration.super.retrieveConfiguration(coordinates);
        }

        String scopeBasedKey = TRACING_CONFIGURATION_DISTRIBUTED_METASTORE_KEY_PREFIX + coordinates.scope().name();

        HashMap<String, TracingConfigurationParameters> scopeBasedTracingConfiguration;

        try {
            scopeBasedTracingConfiguration = ctx.distributedMetastorage().read(scopeBasedKey);
        }
        catch (IgniteCheckedException e) {
            LT.warn(
                log,
                e,
                "Failed to retrieve tracing configuration. Default value will be used.",
                false,
                true);

            // In case of exception during retrieving configuration from metastorage — use scope specific default one.
            return TracingConfiguration.super.retrieveConfiguration(coordinates);
        }

        // If the configuration was not found — use scope specific default one.
        if (scopeBasedTracingConfiguration == null)
            return TracingConfiguration.super.retrieveConfiguration(coordinates);

        // Retrieving scope + label specific tracing configuration.
        TracingConfigurationParameters lbBasedTracingConfiguration =
            scopeBasedTracingConfiguration.get(coordinates.label());

        // If scope + label specific was found — use it.
        if (lbBasedTracingConfiguration != null)
            return lbBasedTracingConfiguration;

        // Retrieving scope specific tracing configuration.
        TracingConfigurationParameters rawScopedTracingConfiguration = scopeBasedTracingConfiguration.get(null);

        // If scope specific was found — use it.
        if (rawScopedTracingConfiguration != null)
            return rawScopedTracingConfiguration;

        // If neither scope + label specific nor just scope specific configuration was found —
        // use scope specific default one.
        return TracingConfiguration.super.retrieveConfiguration(coordinates);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Map<TracingConfigurationCoordinates, TracingConfigurationParameters> retrieveConfigurations() {
        DistributedMetaStorage metaStore;
        try {
            metaStore = ctx.distributedMetastorage();
        }
        catch (Exception e) {
            log.warning("Failed to retrieve tracing configuration — meta storage is not available.");

            // TODO: 04.05.20 default values should go here instead of empty map.
            return Collections.emptyMap();
        }

        if (metaStore == null) {
            log.warning("Failed to retrieve tracing configuration — meta storage is not available.");

            // TODO: 04.05.20 default values should go here instead of empty map.
            return Collections.emptyMap();
        }

        Map<TracingConfigurationCoordinates, TracingConfigurationParameters> res = new HashMap<>();

        for (Scope scope : Scope.values()) {
            String scopeBasedKey = TRACING_CONFIGURATION_DISTRIBUTED_METASTORE_KEY_PREFIX + scope.name();

            try {
                for (Map.Entry<String, TracingConfigurationParameters> entry :
                    ((Map<String, TracingConfigurationParameters>)metaStore.read(scopeBasedKey)).entrySet()) {
                    res.put(
                        new TracingConfigurationCoordinates.Builder(scope).withLabel(entry.getKey()).build(),
                        entry.getValue());
                }
            }
            catch (IgniteCheckedException e) {
                LT.warn(log, "Failed to retrieve tracing configuration");
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void restoreDefaultConfiguration(@NotNull TracingConfigurationCoordinates coordinates) {
        // TODO: 04.05.20
    }
}

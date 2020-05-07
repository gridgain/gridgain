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

package org.apache.ignite.internal.visor.tracing.configuration;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.processors.tracing.Scope;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationCoordinates;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationParameters;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;

/**
 * Task that will collect and update tracing configuration.
 */
@GridInternal
@GridVisorManagementTask
public class VisorTracingConfigurationTask
    extends VisorOneNodeTask<VisorTracingConfigurationTaskArg, VisorTracingConfigurationTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorTracingConfigurationJob job(VisorTracingConfigurationTaskArg arg) {
        return new VisorTracingConfigurationJob(arg, debug);
    }

    /**
     * Job that will collect and update tracing configuration.
     */
    private static class VisorTracingConfigurationJob extends VisorJob<VisorTracingConfigurationTaskArg, VisorTracingConfigurationTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorTracingConfigurationJob(VisorTracingConfigurationTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected @NotNull VisorTracingConfigurationTaskResult run(
            VisorTracingConfigurationTaskArg arg) throws IgniteException {
            switch (arg.operation()) {
                case RETRIEVE_ALL:
                    return retrieveAll();

                case RETRIEVE:
                    return retrieve(arg.scope(), arg.label());

                case RESET:
                    return reset(arg.scope(), arg.label());

                case RESET_ALL:
                    return resetAll(arg.scope());

                case APPLY:
                    return apply(arg.scope(), arg.label(), arg.samplingRate(), arg.supportedScopes());

                default: {
                    assert false; // We should never get here.

                    return retrieveAll(); // Just in case.
                }
            }
        }

        /**
         * Retrieve tracing configuration.
         *
         * @return Tracing configuration as {@link VisorTracingConfigurationTaskResult} instance.
         */
        private @NotNull VisorTracingConfigurationTaskResult retrieveAll() {
            Map<TracingConfigurationCoordinates, TracingConfigurationParameters> tracingConfigurations =
                ignite.tracingConfiguration().retrieveConfigurations();

            VisorTracingConfigurationTaskResult res = new VisorTracingConfigurationTaskResult();

            for (Map.Entry<TracingConfigurationCoordinates, TracingConfigurationParameters> entry: tracingConfigurations.entrySet()) {
                res.add(entry.getKey(), entry.getValue());
            }

            return res;
        }

        /**
         * Retrieve scope specific and optionally label specific tracing configuration.
         *
         * @param scope Scope.
         * @param lb Label
         * @return Scope specific and optionally label specific tracing configuration as
         *  {@link VisorTracingConfigurationTaskResult} instance.
         */
        private @NotNull VisorTracingConfigurationTaskResult retrieve(
            @NotNull Scope scope,
            @Nullable String lb)
        {
            TracingConfigurationCoordinates coordinates =
                new TracingConfigurationCoordinates.Builder(scope).withLabel(lb).build();

            TracingConfigurationParameters updatedParameters =
                ignite.tracingConfiguration().retrieveConfiguration(
                    new TracingConfigurationCoordinates.Builder(scope).withLabel(lb).build());

            VisorTracingConfigurationTaskResult res = new VisorTracingConfigurationTaskResult();

            res.add(coordinates, updatedParameters);

            return res;
        }

        /**
         * Reset scope specific and optionally label specific tracing configuration.
         *
         * @param scope Scope.
         * @param lb Label.
         * @return Reseted scope specific and optionally label specific tracing configuration  as
         *  {@link VisorTracingConfigurationTaskResult} instance.
         */
        private @NotNull VisorTracingConfigurationTaskResult reset(
            @NotNull Scope scope,
            @Nullable String lb)
        {
            ignite.tracingConfiguration().restoreDefaultConfiguration(
                new TracingConfigurationCoordinates.Builder(scope).withLabel(lb).build());

            return retrieve(scope, lb);
        }

        /**
         * Reset tracing configuration, or optionally scope specific tracing configuration.
         *
         * @param scope Scope.
         * @return Tracing configuration as {@link VisorTracingConfigurationTaskResult} instance.
         */
        private @NotNull VisorTracingConfigurationTaskResult resetAll(@Nullable Scope scope) {
            // TODO: 07.05.20

            return retrieveAll();
        }

        /**
         * Apply new tracing configuration.
         *
         * @param scope Scope.
         * @param lb Label.
         * @param samplingRate Sampling rate.
         * @param supportedScopes Set of supported scopes.
         * @return Updated scope specific and optionally label specific tracing configuration  as
         *  {@link VisorTracingConfigurationTaskResult} instance.
         */
        private @NotNull VisorTracingConfigurationTaskResult apply(
            @NotNull Scope scope,
            @Nullable String lb,
            @Nullable Double samplingRate,
            @Nullable Set<Scope> supportedScopes)
        {
            TracingConfigurationCoordinates coordinates =
                new TracingConfigurationCoordinates.Builder(scope).withLabel(lb).build();

            TracingConfigurationParameters.Builder parametersBuilder = new TracingConfigurationParameters.Builder();

            if (samplingRate != null)
                parametersBuilder.withSamplingRate(samplingRate);

            if (supportedScopes != null)
                parametersBuilder.withSupportedScopes(supportedScopes);

            ignite.tracingConfiguration().addConfiguration(coordinates, parametersBuilder.build());

            return retrieve(scope, lb);
        }


        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorTracingConfigurationJob.class, this);
        }
    }
}

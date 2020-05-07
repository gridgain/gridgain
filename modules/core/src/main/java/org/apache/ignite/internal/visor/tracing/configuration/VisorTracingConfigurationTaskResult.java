/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationCoordinates;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationParameters;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result for {@link VisorTracingConfigurationTask}.
 */
public class VisorTracingConfigurationTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Retrieved reseted or updated tracing configuration. */
    private List<VisorTracingConfigurationItem> tracingConfigurations = new ArrayList();

    /**
     * Default constructor.
     */
    public VisorTracingConfigurationTaskResult() {
        // No-op.
    }

    /**
     * Add coordinates and parameters pair to the result.
     *
     * @param coordinates {@link TracingConfigurationCoordinates} instance.
     * @param parameters {@link TracingConfigurationParameters} instance.
     */
    public void add(TracingConfigurationCoordinates coordinates, TracingConfigurationParameters parameters) {
        tracingConfigurations.add(new VisorTracingConfigurationItem(
            coordinates.scope(),
            coordinates.label(),
            parameters.samplingRate(),
            parameters.supportedScopes()
        ));
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, tracingConfigurations);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        tracingConfigurations = (List)U.readCollection(in);
    }

    /**
     * Fills printer {@link Consumer <String>} by string view of this class.
     */
    public void print(Consumer<String> printer) {
        // TODO: 07.05.20 Add pretty printing.
        for (VisorTracingConfigurationItem tracingConfiguration : tracingConfigurations) {
            StringBuilder tracingConfigurationLine = new StringBuilder();

            tracingConfigurationLine.append("Scope: ");
            tracingConfigurationLine.append(tracingConfiguration.scope().name());

            if (tracingConfiguration.label() != null) {
                tracingConfigurationLine.append(", Label: '");
                tracingConfigurationLine.append(tracingConfiguration.label());
                tracingConfigurationLine.append(".");
            }

            tracingConfigurationLine.append(", Sampling Rate: ");
            tracingConfigurationLine.append(tracingConfiguration.samplingRate());

            if (tracingConfiguration.supportedScopes() != null && !tracingConfiguration.supportedScopes().isEmpty()) {
                tracingConfigurationLine.append(", Supported Scopes: ");
                tracingConfigurationLine.append(Arrays.toString(tracingConfiguration.supportedScopes().toArray()));
            }

            printer.accept(tracingConfigurationLine.toString());
        }
    }
}

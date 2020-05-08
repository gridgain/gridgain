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

package org.apache.ignite.internal.processors.monitoring.opencensus;

import java.util.Collections;
import org.apache.ignite.internal.processors.tracing.TracingSpi;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfiguration;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationCoordinates;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.junit.Test;

import static org.apache.ignite.internal.processors.tracing.Scope.COMMUNICATION;
import static org.apache.ignite.internal.processors.tracing.Scope.TX;

/**
 * Tests for OpenCensus based {@link TracingConfiguration#get(TracingConfigurationCoordinates)}.
 */
public class OpenCensusTracingConfigurationGetTest extends AbstractTracingTest {
    /** {@inheritDoc} */
    @Override protected TracingSpi getTracingSpi() {
        return new OpenCensusTracingSpi();
    }

    /**
     * Ensure that label specific configuration retrieval returns default scope specific if there's
     * neither corresponding custom label specific nor corresponding custom scope specific.
     */
    @Test
    public void testThatLabelSpecificConfigurationRetrievalReturnsDefaultIfCustomConfigurationNotSet() {
        assertEquals(
            TracingConfiguration.DEFAULT_TX_CONFIGURATION,
            grid(0).tracingConfiguration().retrieve(
                new TracingConfigurationCoordinates.Builder(TX).withLabel("label").build()));
    }

    /**
     * Ensure that label specific configuration retrieval returns custom scope specific if there's no
     * corresponding custom label specific one.
     */
    @Test
    public void testThatLabelSpecificConfigurationRetrievalReturnsCustomScopeSpecificIfLabelSpecificIsNotSet() {
        TracingConfigurationCoordinates coords =
            new TracingConfigurationCoordinates.Builder(TX).build();

        TracingConfigurationParameters expScopeSpecificParameters =
            new TracingConfigurationParameters.Builder().
                withSamplingRate(0.2).
                withSupportedScopes(Collections.singleton(COMMUNICATION)).build();

        grid(0).tracingConfiguration().apply(coords, expScopeSpecificParameters);

        assertEquals(
            expScopeSpecificParameters,
            grid(0).tracingConfiguration().retrieve(
                new TracingConfigurationCoordinates.Builder(TX).withLabel("label").build()));
    }

    /**
     * Ensure that label specific configuration retrieval returns custom label specific is such one is present.
     */
    @Test
    public void testThatLabelSpecificConfigurationRetrievalReturnsLabelSpecificOne() {
        TracingConfigurationCoordinates coords =
            new TracingConfigurationCoordinates.Builder(TX).withLabel("label").build();

        TracingConfigurationParameters expScopeSpecificParameters =
            new TracingConfigurationParameters.Builder().
                withSamplingRate(0.35).
                withSupportedScopes(Collections.singleton(COMMUNICATION)).build();

        grid(0).tracingConfiguration().apply(coords, expScopeSpecificParameters);

        assertEquals(
            expScopeSpecificParameters,
            grid(0).tracingConfiguration().retrieve(coords));
    }

    /**
     * Ensure that scope specific configuration retrieval returns default scope specific if there's no corresponding
     * custom specific one.
     */
    @Test
    public void testThatScopeSpecificConfigurationRetrievalReturnsDefaultOneIfCustomConfigurationNotSet() {
        assertEquals(
            TracingConfiguration.DEFAULT_TX_CONFIGURATION,
            grid(0).tracingConfiguration().retrieve(
                new TracingConfigurationCoordinates.Builder(TX).build()));
    }

    /**
     * Ensure that scope specific configuration retrieval returns corresponding custom specific one if it's available.
     */
    @Test
    public void testThatScopeSpecificConfigurationRetrievalReturnsCustomScopeSpecific() {
        TracingConfigurationCoordinates coords =
            new TracingConfigurationCoordinates.Builder(TX).build();

        TracingConfigurationParameters expScopeSpecificParameters =
            new TracingConfigurationParameters.Builder().
                withSamplingRate(0.2).
                withSupportedScopes(Collections.singleton(COMMUNICATION)).build();

        grid(0).tracingConfiguration().apply(coords, expScopeSpecificParameters);

        assertEquals(
            expScopeSpecificParameters,
            grid(0).tracingConfiguration().retrieve(coords));
    }

    /**
     * Ensure that scope specific configuration retrieval returns corresponding custom specific one if it's available
     * and ignores label specific one.
     */
    @Test
    public void testThatScopeSpecificConfigurationRetrievalReturnsScopeSpecificEventIfLabelSpecificIsSet() {
        TracingConfigurationCoordinates scopeSpecificCoords =
            new TracingConfigurationCoordinates.Builder(TX).build();

        TracingConfigurationCoordinates lbSpecificCoords =
            new TracingConfigurationCoordinates.Builder(TX).withLabel("label").build();

        TracingConfigurationParameters expScopeSpecificParameters =
            new TracingConfigurationParameters.Builder().
                withSamplingRate(0.35).
                withSupportedScopes(Collections.singleton(COMMUNICATION)).build();

        grid(0).tracingConfiguration().apply(lbSpecificCoords, expScopeSpecificParameters);

        assertEquals(
            TracingConfiguration.DEFAULT_TX_CONFIGURATION,
            grid(0).tracingConfiguration().retrieve(scopeSpecificCoords));
    }
}

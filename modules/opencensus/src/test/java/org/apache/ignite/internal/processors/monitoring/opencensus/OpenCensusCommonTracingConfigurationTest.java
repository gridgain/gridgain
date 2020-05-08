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

package org.apache.ignite.internal.processors.monitoring.opencensus;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.tracing.Scope;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationCoordinates;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationParameters;
import org.apache.ignite.testframework.junits.SystemPropertiesList;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_DISTRIBUTED_META_STORAGE_FEATURE;
import static org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationParameters.SAMPLING_RATE_NEVER;

public class OpenCensusCommonTracingConfigurationTest extends GridCommonAbstractTest {

    /**
     * Ensure that it's possible to override tracing coniguration at runtime with any cluster node including client one.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testThatItsPossibleToOverrideTracingConfigurationAtRuntimeWithinAnyClusterNode() throws Exception {

    }

    /**
     * Ensure that default tracing configuration is used if distributed metastorage is disabled.
     * Distributed metastorage is used to store tracing configuration and propagate it over cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @SystemPropertiesList({
        @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "false"),
        @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_FEATURE, value = "false"),
        @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "false")
    })
    public void testThatDefaultTracingConfigurationIsUsedIfMetastorageIsDisabled() throws Exception {
        Ignite node = startGrid();

        // TODO: 07.05.20
//        assertFalse(node.tracingConfiguration().apply(
//            new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
//            new TracingConfigurationParameters.Builder().withSamplingRate(SAMPLING_RATE_NEVER).build()));


        // TODO: 04.05.20 check log message here.
    }
}

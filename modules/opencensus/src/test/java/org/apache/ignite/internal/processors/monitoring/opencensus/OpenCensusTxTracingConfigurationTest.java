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
import java.util.stream.Collectors;
import io.opencensus.trace.SpanId;
import io.opencensus.trace.export.SpanData;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.tracing.Scope;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationCoordinates;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationParameters;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;
import static org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationParameters.SAMPLING_RATE_NEVER;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests for transaction tracing configuration.
 */
public class OpenCensusTxTracingConfigurationTest extends AbstractOpenCensusTracingTest {

    @Test
    public void testTxConfigurationSamplingRateNeverPreventsTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        client.tracingConfiguration().addConfiguration(
            new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
            new TracingConfigurationParameters.Builder().withSamplingRate(SAMPLING_RATE_NEVER).build());

        client.transactions().txStart(PESSIMISTIC, SERIALIZABLE).commit();

        handler().flush();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> SpanType.TX.traceName().equals(span.getName()))
            .collect(Collectors.toList());

        assertTrue(gotSpans.isEmpty());
    }

    @Test
    public void testTxConfigurationSamplingRateAlwaysEnablesTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        client.tracingConfiguration().addConfiguration(
            new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
            new TracingConfigurationParameters.Builder().withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        client.transactions().txStart(PESSIMISTIC, SERIALIZABLE).commit();

        handler().flush();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> SpanType.TX.traceName().equals(span.getName()))
            .collect(Collectors.toList());

        assertEquals(1, gotSpans.size());
    }

    @Test
    public void testTxConfigurationSamplingRateHalfSamplesSomethingAboutHalfTransactions() throws Exception {
        IgniteEx client = startGrid("client");

        client.tracingConfiguration().addConfiguration(
            new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
            new TracingConfigurationParameters.Builder().withSamplingRate(0.5).build());

        final int txAmount = 10;

        for (int i = 0; i < txAmount; i++)
            client.transactions().txStart(PESSIMISTIC, SERIALIZABLE).commit();

        handler().flush();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> SpanType.TX.traceName().equals(span.getName()))
            .collect(Collectors.toList());

        // Cause of probability nature of sampling it's not possible to check that 0.5 sampling rate will end with
        // 5 sampling transactions out of {@code txAmount},
        // so we just check that some and not all transactions were traced.
        assertTrue(!gotSpans.isEmpty() && gotSpans.size() < txAmount);
    }

    @Test
    public void testTxTraceDoesNotIncludeCommunicationTracesInCaseOfEmptySupportedScopes() throws Exception {
        IgniteEx client = startGrid("client");

        client.tracingConfiguration().addConfiguration(
            new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
            new TracingConfigurationParameters.Builder().withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        Transaction tx = client.transactions().txStart(PESSIMISTIC, SERIALIZABLE);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);

        tx.commit();

        handler().flush();

        SpanId parentSpanId = handler().allSpans()
            .filter(span -> SpanType.TX_NEAR_PREPARE.traceName().equals(span.getName()))
            .collect(Collectors.toList()).get(0).getContext().getSpanId();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> parentSpanId.equals(span.getParentSpanId()) &&
                SpanType.COMMUNICATION_SOCKET_WRITE.traceName().equals(span.getName()))
            .collect(Collectors.toList());

        assertTrue(gotSpans.isEmpty());
    }

    @Test
    public void testTxTraceIncludesCommunicationTracesInCaseOfCommunicationScopeInTxSupportedScopes() throws Exception {
        IgniteEx client = startGrid("client");

        client.tracingConfiguration().addConfiguration(
            new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
            new TracingConfigurationParameters.Builder().
                withSamplingRate(SAMPLING_RATE_ALWAYS).
                withSupportedScopes(Collections.singleton(Scope.COMMUNICATION)).
                build());

        Transaction tx = client.transactions().txStart(PESSIMISTIC, SERIALIZABLE);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);

        tx.commit();

        handler().flush();

        SpanId parentSpanId = handler().allSpans()
            .filter(span -> SpanType.TX_NEAR_PREPARE.traceName().equals(span.getName()))
            .collect(Collectors.toList()).get(0).getContext().getSpanId();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> parentSpanId.equals(span.getParentSpanId()) &&
                SpanType.COMMUNICATION_SOCKET_WRITE.traceName().equals(span.getName()))
            .collect(Collectors.toList());

        assertFalse(gotSpans.isEmpty());
    }

    @Test
    public void testThatLabelSpecificConfigurationIsUsedWheneverPossible() throws Exception {
        IgniteEx client = startGrid("client");

        final String txLabelToBeTraced = "label1";

        final String txLabelNotToBeTraced = "label2";

        client.tracingConfiguration().addConfiguration(
            new TracingConfigurationCoordinates.Builder(Scope.TX).withLabel(txLabelToBeTraced).build(),
            new TracingConfigurationParameters.Builder().withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        client.transactions().withLabel(txLabelToBeTraced).txStart(PESSIMISTIC, SERIALIZABLE).commit();

        handler().flush();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> SpanType.TX.traceName().equals(span.getName()))
            .collect(Collectors.toList());

        assertEquals(1, gotSpans.size());

        // Not to be traced, cause there's neither tracing configuration with given label
        // nor scope specific tx configuration. In that case default tx tracing configuration will be used that
        // actually disables tracing.
        client.transactions().withLabel(txLabelNotToBeTraced).txStart(PESSIMISTIC, SERIALIZABLE).commit();

        handler().flush();

        gotSpans = handler().allSpans()
            .filter(span -> SpanType.TX.traceName().equals(span.getName()))
            .collect(Collectors.toList());

        // Still only one, previously detected, span is expected.
        assertEquals(1, gotSpans.size());
    }

    @Test
    public void testThatScopeSpecificConfigurationIsUsedIfLabelSpecificNotFound() throws Exception {
        IgniteEx client = startGrid("client");

        client.tracingConfiguration().addConfiguration(
            new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
            new TracingConfigurationParameters.Builder().withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        client.transactions().withLabel("label1").txStart(PESSIMISTIC, SERIALIZABLE).commit();

        handler().flush();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> SpanType.TX.traceName().equals(span.getName()))
            .collect(Collectors.toList());

        assertEquals(1, gotSpans.size());
    }

    @Test
    public void testThatDefaultConfigurationIsUsedIfScopeBasedNotFoundAndThatByDefaultTxTracingIsDisabled()
        throws Exception {
        IgniteEx client = startGrid("client");

        client.transactions().withLabel("label1").txStart(PESSIMISTIC, SERIALIZABLE).commit();

        handler().flush();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> SpanType.TX.traceName().equals(span.getName()))
            .collect(Collectors.toList());

        assertTrue(gotSpans.isEmpty());
    }
}

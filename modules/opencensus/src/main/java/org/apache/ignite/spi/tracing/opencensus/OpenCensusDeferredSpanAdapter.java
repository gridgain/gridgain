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

package org.apache.ignite.spi.tracing.opencensus;

//import org.apache.ignite.internal.processors.tracing.DefferedSpan;
//import org.apache.ignite.internal.processors.tracing.Scope;
import org.apache.ignite.internal.processors.tracing.DeferredSpan;
import org.apache.ignite.internal.processors.tracing.Scope;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanStatus;
import org.apache.ignite.internal.processors.tracing.SpanType;

import java.util.Map;
import java.util.Set;

public class OpenCensusDeferredSpanAdapter implements Span, DeferredSpan {

    private byte[] serializedSpan;

    public OpenCensusDeferredSpanAdapter(byte[] serializedSpan) {
        this.serializedSpan = serializedSpan;
    }

    /**
     * @return Serialized span.
     */
    @Override public byte[] serializedSpan() {
        return serializedSpan;
    }

    @Override public Span addTag(String tagName, String tagVal) {
        return null;
    }

    @Override public Span addTag(String tagName, long tagVal) {
        return null;
    }

    @Override public Span addLog(String logDesc) {
        return null;
    }

    @Override public Span addLog(String logDesc, Map<String, String> attributes) {
        return null;
    }

    @Override public Span setStatus(SpanStatus spanStatus) {
        return null;
    }

    @Override public Span end() {
        return null;
    }

    @Override public boolean isEnded() {
        return false;
    }

    @Override public SpanType type() {
        return null;
    }

    @Override public Set<Scope> includedScopes() {
        return null;
    }

    @Override public boolean isChainable(Scope scope) {
        return false;
    }
}

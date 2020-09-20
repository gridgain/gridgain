/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.tracing;

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.SpanStatus;

/**
 * Noop and null-safe implementation of Span.
 */
public class NoopSpan implements Span {
    /** Instance. */
    public static final Span INSTANCE = new NoopSpan();

    /**
     * Constructor.
     */
    private NoopSpan(){

    }

    /** {@inheritDoc} */
    @Override public Span addTag(String tagName, Supplier<String> tagValSupplier) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public Span addSensitiveTag(String tagName, Supplier<String> tagValSupplier) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public Span addSensitiveTagOrLog(
        String tagName,
        SpanType directParentSpan,
        Supplier<String> tagValSupplier
    ) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public Span addTagOrLog(
        String tagName,
        SpanType directParentSpan,
        Supplier<String> tagValSupplier
    ) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public Span addLog(Supplier<String> logDescSupplier) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public Span setStatus(SpanStatus spanStatus) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public Span end() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean isEnded() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public SpanType type() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Set<Scope> includedScopes() {
        return Collections.emptySet();
    }
}

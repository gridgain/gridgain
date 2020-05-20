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

import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Manager for {@link Span} instances.
 */
public interface SpanManager {
    /**
     * Creates Span with given name.
     *
     * @param spanType Type of span to create.
     */
    public default Span create(@NotNull SpanType spanType) {
        return create(spanType, (Span)null);
    }

    /**
     * Creates Span given name and explicit parent.
     *
     * @param spanType Type of span to create.
     * @param parentSpan Parent span.
     * @return Created span.
     */
    public Span create(@NotNull SpanType spanType, @Nullable Span parentSpan);

    /**
     * Creates Span given name and explicit parent.
     *
     * @param spanType Type of span to create.
     * @param serializedSpan Parent span as serialized bytes.
     * @return Created span.
     */
    public Span create(@NotNull SpanType spanType, @Nullable byte[] serializedSpan);

    /**
     * Creates Span given name and explicit parent.
     *
     * @param spanType Type of span to create.
     * @param parentSpan Parent span.
     * @param samplingRate Number between 0 and 1 that more or less reflects the probability of sampling specific trace.
     * 0 and 1 have special meaning here, 0 means never 1 means always. Default value is 0 (never).
     * @param includedScopes Set of {@link Scope} that defines which sub-traces will be included in given trace.
     *  In other words, if child's span scope is equals to parent's scope
     *  or it belongs to the parent's span included scopes, then given child span will be attached to the current trace,
     *  otherwise it'll be skipped.
     *  See {@link Span#isChainable(org.apache.ignite.internal.processors.tracing.Scope)} for more details.
     * @return Created span.
     */
    public @NotNull Span create (
        @NotNull SpanType spanType,
        @Nullable Span parentSpan,
        double samplingRate,
        @NotNull Set<Scope> includedScopes);

    /**
     * Serializes span to byte array to send context over network.
     *
     * @param span Span.
     */
    public byte[] serialize(@NotNull Span span);
}

/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.rest.request;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Request for the {@code PROBE} REST command. Carries the optional
 * {@code kind} sub-command that selects the probe semantics:
 * <ul>
 *     <li>{@code null} — bare-GET form, backward-compatible kernel-started check.</li>
 *     <li>{@code "liveness"} — Jetty bound + JVM kernel started.</li>
 *     <li>{@code "readiness"} — k8s readiness/startup gate (initial rebalance complete,
 *         cluster active, not in maintenance mode).</li>
 * </ul>
 * <p>Unknown {@code kind} values are classified at the handler layer as
 * {@code STATUS_FAILED} with an explicit message; lookup is case-insensitive.</p>
 */
public class GridRestProbeRequest extends GridRestRequest {
    /** Optional kind: {@code "liveness"}, {@code "readiness"}, or {@code null} (bare-GET). */
    private String kind;

    /**
     * @return Kind sub-command, or {@code null} for the bare-GET form.
     */
    @Nullable public String kind() {
        return kind;
    }

    /**
     * @param kind Kind sub-command.
     */
    public void kind(@Nullable String kind) {
        this.kind = kind;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestProbeRequest.class, this, super.toString());
    }
}

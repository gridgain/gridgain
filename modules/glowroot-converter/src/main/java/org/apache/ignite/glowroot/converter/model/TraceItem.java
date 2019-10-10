/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.glowroot.converter.model;

/**
 * Basic class for all glowroot trace items.
 */
public abstract class TraceItem {

    /**
     * Within context of GridGain CE Gloowroot plugin every trace item is bounded to glowroot transaction.
     * Glowroot transaction may or may not be bounded to an Ignite transaction.
     *   Ignite Transaction -> Glowroot Transaction -> 0..many traces.
     *   No Ignite Transaction -> Glowroot Transaction -> 0..1 trace.
     */
    private final String glowrootTxId;

    /**
     * Trace duration in nanoseconds
     */
    private final long durationNanos;

    /**
     * Trace offset in nanoseconds from the begining of glowroot transaction.
     */
    private final long offsetNanos;

    /**
     * Constructor.
     *
     * @param txId Glowroot transaction id.
     * @param durationNanos Trace duration in nanoseconds.
     * @param offsetNanos Trace offset in nanoseconds from the begining of glowroot transaction.
     */
    protected TraceItem(String txId, long durationNanos, long offsetNanos) {
        glowrootTxId = txId;
        this.durationNanos = durationNanos;
        this.offsetNanos = offsetNanos;
    }

    /**
     * @return gloowroot tx id.
     */
    public String glowrootTxId() {
        return glowrootTxId;
    }

    /**
     * @return Duration nanos.
     */
    public long durationNanos() {
        return durationNanos;
    }

    /**
     * @return Offset nanos.
     */
    public long offsetNanos() {
        return offsetNanos;
    }
}

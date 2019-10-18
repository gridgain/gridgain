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
     * Within context of GridGain CE Gloowroot plugin every trace item is bounded to glowroot transaction. Glowroot
     * transaction may or may not be bounded to an Ignite transaction. Ignite Transaction -> Glowroot Transaction ->
     * 0..many traces. No Ignite Transaction -> Glowroot Transaction -> 0..1 trace.
     */
    private final GlowrootTransactionMeta glowrootTx;

    /** Trace duration in nanoseconds. **/
    private final long durationNanos;

    /** Trace offset in nanoseconds from the begining of glowroot transaction. **/
    private final long offsetNanos;

    /**
     * Constructor.
     *
     * @param glowrootTx Glowroot transaction.
     * @param durationNanos Trace duration in nanoseconds.
     * @param offsetNanos Trace offset in nanoseconds from the begining of glowroot transaction.
     */
    protected TraceItem(GlowrootTransactionMeta glowrootTx, long durationNanos, long offsetNanos) {
        this.glowrootTx = glowrootTx;
        this.durationNanos = durationNanos;
        this.offsetNanos = offsetNanos;
    }

    /**
     * @return gloowroot transaction.
     */
    public GlowrootTransactionMeta glowrootTransaction() {
        return glowrootTx;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return "TraceItem{" +
            "glowrootTx=" + glowrootTx +
            ", durationNanos=" + durationNanos +
            ", offsetNanos=" + offsetNanos +
            '}';
    }
}

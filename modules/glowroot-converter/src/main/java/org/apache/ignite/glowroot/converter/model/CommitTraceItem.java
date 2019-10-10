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
 * Ignite transaction commit trace item produced by {@code org.apache.ignite.glowroot.TransactionAspect}
 */
public final class CommitTraceItem extends TraceItem {

    /** Label.**/
    private final String label;

    /**
     * Constructor.
     *
     * @param txId Glowroot transaction id.
     * @param durationNanos Trace duration in nanoseconds.
     * @param offsetNanos Trace offset in nanoseconds from the begining of glowroot transaction.
     * @param label Label.
     */
    public CommitTraceItem(String txId, long durationNanos, long offsetNanos, String label) {
        super(txId, durationNanos, offsetNanos);

        this.label = label;
    }

    /**
     * @return Label.
     */
    public String label() {
        return label;
    }
}

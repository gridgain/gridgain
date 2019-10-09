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

package org.apache.ignite.glowroot.converter.model;public abstract class TraceItem {

    private final String glowrootTxId;

    private final long durationNanos;

    private final long offsetNanos;

    public TraceItem(String txId, long durationNanos, long offsetNanos) {
        this.glowrootTxId = txId;
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

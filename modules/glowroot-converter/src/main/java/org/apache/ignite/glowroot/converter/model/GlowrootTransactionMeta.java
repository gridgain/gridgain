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
 * Glowroot transaction metadata.
 */
public final class GlowrootTransactionMeta {

    /** Glowroot transaction id.**/
    private final String id;

    /** Glowroot transaction start time. **/
    private final long startTime;

    /** Glowroot transaction duration. **/
    private final long durationNanos;

    /**
     * Constructor
     * @param id Glowroot transaction id
     * @param startTime Glowroot transaction start time.
     * @param durationNanos Glowroot transaction duration.
     */
    public GlowrootTransactionMeta(String id, long startTime, long durationNanos) {
        this.id = id;
        this.startTime = startTime;
        this.durationNanos = durationNanos;
    }

    /**
     * @return Id.
     */
    public String id() {
        return id;
    }

    /**
     * @return Start time.
     */
    public long startTime() {
        return startTime;
    }

    /**
     * @return Duration nanos.
     */
    public long durationNanos() {
        return durationNanos;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GlowrootTransactionMeta{" +
            "id='" + id + '\'' +
            ", startTime=" + startTime +
            ", durationNanos=" + durationNanos +
            '}';
    }
}

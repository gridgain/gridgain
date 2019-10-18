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
 * Cache query trace item produced by {@code org.apache.ignite.glowroot.CacheAspect}
 */
public final class CacheQueryTraceItem extends TraceItem {

    /** Cache name. **/
    private final String cacheName;

    /** Query. **/
    private final String qry;

    /**
     * Constructor.
     *
     * @param glowrootTx Glowroot transaction.
     * @param durationNanos Trace duration in nanoseconds.
     * @param offsetNanos Trace offset in nanoseconds from the begining of glowroot transaction.
     * @param cacheName Cache name.
     * @param qry Query.
     */
    public CacheQueryTraceItem(GlowrootTransactionMeta glowrootTx, long durationNanos, long offsetNanos,
        String cacheName, String qry) {
        super(glowrootTx, durationNanos, offsetNanos);

        this.cacheName = cacheName;
        this.qry = qry;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Query.
     */
    public String query() {
        return qry;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CacheQueryTraceItem{" +
            "cacheName='" + cacheName + '\'' +
            ", qry='" + qry + '\'' +
            '}';
    }
}

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
 * Cache trace item produced by {@code org.apache.ignite.glowroot.CacheAspect}
 */
public final class CacheTraceItem extends TraceItem {

    /** Cache name. **/
    private final String cacheName;

    /** Cache operation. e.g, put, get, etc. **/
    private final String operation;

    /** Comma separated list of arguments meta: argument types, etc. **/
    private final String args;

    /**
     * Constructor.
     *
     * @param glowrootTx Glowroot transaction.
     * @param durationNanos Trace duration in nanoseconds.
     * @param offsetNanos Trace offset in nanoseconds from the begining of glowroot transaction.
     * @param cacheName Cache name.
     * @param operation Cache operation e.g, put, get, etc.
     * @param args  Comma separated list of arguments meta: argument types, etc.
     */
    public CacheTraceItem(GlowrootTransactionMeta glowrootTx, long durationNanos, long offsetNanos, String cacheName,
        String operation, String args) {
        super(glowrootTx, durationNanos, offsetNanos);

        this.cacheName = cacheName;
        this.operation = operation;
        this.args = args;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Operation.
     */
    public String operation() {
        return operation;
    }

    /**
     * @return Args.
     */
    public String args() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CacheTraceItem{" +
            "cacheName='" + cacheName + '\'' +
            ", operation='" + operation + '\'' +
            ", args='" + args + '\'' +
            '}';
    }
}

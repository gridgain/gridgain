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

package org.apache.ignite.glowroot.converter.model;public final class CacheTraceItem extends TraceItem {

    private final String cacheName;

    private final String operation;

    private final String args;

    public CacheTraceItem(String txId, long durationNanos, long offsetNanos, String cacheName, String operation,
        String args) {
        super(txId, durationNanos, offsetNanos);

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
}

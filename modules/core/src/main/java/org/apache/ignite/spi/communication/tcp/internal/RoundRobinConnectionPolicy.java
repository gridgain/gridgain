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

package org.apache.ignite.spi.communication.tcp.internal;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Round robin connection policy.
 */
public class RoundRobinConnectionPolicy implements ConnectionPolicy {
    /** Maximal connections number. */
    private final int cnt;

    /** */
    private final AtomicInteger counter = new AtomicInteger();

    /**
     * @param cnt Maximal connections number.
     */
    public RoundRobinConnectionPolicy(int cnt) {
        this.cnt = cnt;
    }

    /** {@inheritDoc} */
    @Override public int connectionIndex() {
        int idx = counter.getAndIncrement() % cnt;

        if (idx < 0)
            idx += cnt;

        return idx;
    }
}

/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.managers.communication;

/** Holder for priority processing. */
public class PriorityWrapper implements Runnable {
    private final Runnable clo;

    private final byte priority;

    private final long initializedTs;

    /** */
    public PriorityWrapper(Runnable clo, byte priority) {
        this.clo = clo;
        this.priority = priority;
        initializedTs = System.currentTimeMillis();
    }

    /** */
    public PriorityWrapper(Runnable clo) {
        this(clo, (byte)0);
    }

    /** */
    @Override public void run() {
        clo.run();
    }

    /**
     * @return Current priority.
     */
    public byte priority() {
        return priority;
    }

    /**
     * This value is used to preserve natural ordering for tasks with the same priority.
     * Because PriorityQueue doesn't guarantee FIFO order for the same priority.
     *
     * @return Initialized ts.
     */
    public long initializedTs() {
        return initializedTs;
    }
}

/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.processors.query.stat;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Statistics gathering context.
 */
public class LocalStatisticsGatheringContext {
    /** Remaining partitions */
    private final Set<Integer> remainingParts;

    /** Done future. */
    private final CompletableFuture<Void> fut;

    /** */
    public LocalStatisticsGatheringContext(Set<Integer> remainingParts) {
        this.remainingParts = new HashSet<>(remainingParts);
        this.fut = new CompletableFuture<>();
    }

    /**
     * Decrement remaining.
     */
    public synchronized void partitionDone(int partId) {
        remainingParts.remove(partId);

        if (remainingParts.isEmpty())
            fut.complete(null);
    }

    /**
     * @return Collection control future.
     */
    public CompletableFuture<Void> future() {
        return fut;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LocalStatisticsGatheringContext.class, this);
    }
}

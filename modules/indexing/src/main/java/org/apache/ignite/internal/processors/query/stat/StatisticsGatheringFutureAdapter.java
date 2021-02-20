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

import org.apache.ignite.internal.util.future.GridFutureAdapter;

import java.util.UUID;

/**
 * Cancellable future adapter with gathering id.
 */
public class StatisticsGatheringFutureAdapter<R> extends GridFutureAdapter<R> implements StatisticsGatheringFuture<R> {
    /** Gathering id. */
    private final UUID gatId;

    /** StatisticsTargets, covered by the future. */
    private final StatisticsTarget[] targets;

    /**
     * Constructor.
     *
     * @param gatId Collection id.
     * @param targets Statistics target, covered by the future.
     */
    public StatisticsGatheringFutureAdapter(UUID gatId, StatisticsTarget[] targets) {
        this.gatId = gatId;
        this.targets = targets;
    }

    /** {@inheritDoc} */
    @Override public UUID gatId() {
        return gatId;
    }

    /** {@inheritDoc} */
    @Override public StatisticsTarget[] targets() {
        return targets;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        boolean res = onDone(null, null, true);

        if (res)
            onCancelled();

        return res;
    }
}

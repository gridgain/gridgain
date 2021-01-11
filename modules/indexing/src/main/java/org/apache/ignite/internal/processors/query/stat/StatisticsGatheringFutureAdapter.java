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
 * Cancellable future adapter.
 */
public class StatisticsGatheringFutureAdapter<R> extends GridFutureAdapter<R> implements StatsCollectionFuture<R> {
    /** Collection id. */
    private final UUID colId;

    /**
     * Constructor.
     *
     * @param colId Collection id.
     */
    public StatisticsGatheringFutureAdapter(UUID colId){
        this.colId = colId;
    }

    /** {@inheritDoc} */
    @Override public UUID colId() {
        return colId;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        boolean res = onDone(null, null, true);

        if (res)
            onCancelled();

        return res;
    }
}

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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.processors.continuous.GridContinuousBatch;
import org.apache.ignite.internal.processors.continuous.GridContinuousQueryBatch;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Buffer for collecting CQ acknowledges before CQ buffer cleanup.
 */
class CacheContinuousQueryAcknowledgeBuffer {
    /** */
    private int size;

    /** */
    @GridToStringInclude
    private Map<Integer, Long> updateCntrs = new HashMap<>();

    /**
     * @param batch Batch.
     * @return Update counters per partition map if acknowledge should be sent.
     */
    @SuppressWarnings("unchecked")
    @Nullable synchronized Map<Integer, Long> onAcknowledged(
        GridContinuousBatch batch) {
        assert batch instanceof GridContinuousQueryBatch;

        size += ((GridContinuousQueryBatch)batch).entriesCount();

        Collection<CacheContinuousQueryEntry> entries = (Collection)batch.collect();

        for (CacheContinuousQueryEntry e : entries)
            addEntry(e);

        return size >= CacheContinuousQueryHandler.ACK_THRESHOLD ? acknowledgeData() : null;
    }

    /**
     * @param e Entry.
     */
    private void addEntry(CacheContinuousQueryEntry e) {
        Long cntr0 = updateCntrs.get(e.partition());

        if (cntr0 == null || e.updateCounter() > cntr0)
            updateCntrs.put(e.partition(), e.updateCounter());
    }

    /**
     * @return Update counters per partition information.
     */
    private Map<Integer, Long> acknowledgeData() {
        assert size > 0;

        Map<Integer, Long> cntrs = new HashMap<>(updateCntrs);

        updateCntrs.clear();

        size = 0;

        return cntrs;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryAcknowledgeBuffer.class, this);
    }
}

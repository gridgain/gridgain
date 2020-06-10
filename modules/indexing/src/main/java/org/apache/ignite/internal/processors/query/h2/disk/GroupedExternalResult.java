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
package org.apache.ignite.internal.processors.query.h2.disk;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import org.h2.engine.Session;
import org.h2.value.Value;
import org.h2.value.ValueRow;

/**
 * Wrapper for spilled groups file.
 */
public class GroupedExternalResult extends AbstractExternalResult<Object> {
    /** Result queue. */
    private Queue<ExternalResultData.Chunk> resQueue;

    /** Chunks comparator. */
    private final Comparator<ExternalResultData.Chunk> chunkCmp;

    /**  Comparator for values within {@link #chunkCmp}. */
    private final Comparator<Value> cmp;

    /**
     * @param ses Session.
     * @param initSize Initial size;
     */
    public GroupedExternalResult(Session ses, long initSize) {
        super(ses, false, 0, Object.class);
        this.cmp = ses.getDatabase().getCompareMode();
        this.chunkCmp = new Comparator<ExternalResultData.Chunk>() {
            @Override public int compare(ExternalResultData.Chunk o1, ExternalResultData.Chunk o2) {
                int c = cmp.compare((Value)o1.currentRow().getKey(), (Value)o2.currentRow().getKey());

                if (c != 0)
                    return c;

                // Compare batches to ensure they emit rows in the arriving order.
                return Long.compare(o1.start(), o2.start());
            }
        };
    }

    /** */
    public Map.Entry<ValueRow, Object[]> next() {
        if (resQueue.isEmpty())
            return null;

        ExternalResultData.Chunk batch = resQueue.poll();

        Map.Entry<ValueRow, Object[]> row = (Map.Entry<ValueRow, Object[]>)batch.currentRow();

        if (batch.next())
            resQueue.offer(batch);

        return row;
    }

    /**
     * @param groups Groups to spill.
     */
    public void spillGroupsToDisk(Map<ValueRow, Object[]> groups) {
        size += groups.size();

        data.store(groups.entrySet());
    }

    /** Invoked after all rows have been spilled to disk.*/
    public void reset() {
        if (resQueue != null) {
            resQueue.clear();

            for (ExternalResultData.Chunk chunk : data.chunks())
                chunk.reset();
        }
        else
            resQueue = new PriorityQueue<>(chunkCmp);

        // Init chunks.
        for (ExternalResultData.Chunk chunk : data.chunks()) {
            if (chunk.next())
                resQueue.offer(chunk);
        }
    }
}

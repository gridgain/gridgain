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

package org.apache.ignite.internal.processors.bulkload;

import org.apache.ignite.internal.util.lang.IgniteClosureX;
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.List;

/**
 * Bulk load processor
 */
public class BulkLoadProcessor {

    /**
     * Converter, which transforms the list of strings parsed from the input stream to the key+value entry to add to
     * the cache.
     */
    private final IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> dataConverter;

    /** Streamer that puts actual key/value into the cache. */
    private final BulkLoadCacheWriter outputStreamer;

    private long updateCnt;

    /** Tracing processor. */

    /** Span of the running query. */

    public BulkLoadProcessor(IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> dataConverter,
                             BulkLoadCacheWriter outputStreamer) {
        this.dataConverter = dataConverter;
        this.outputStreamer = outputStreamer;
        this.updateCnt = 0;

    }

    /**
     * Load read data ito GridGain cluster
     * @param batch
     */
    public void processBatch(List<List<String>> batch) {
        for (List<String> record : batch) {
            IgniteBiTuple<?, ?> kv = dataConverter.apply(record);
            outputStreamer.apply(kv);
            this.updateCnt++;
        }
        outputStreamer.flush();
    }

    /**
     * @return number of inserted rows
     */
    public long getUpdateCnt() {
        return this.updateCnt;
    }
}

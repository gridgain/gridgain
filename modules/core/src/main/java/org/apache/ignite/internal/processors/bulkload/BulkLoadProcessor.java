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

import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.NoopSpan;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.internal.util.lang.IgniteClosureX;
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.List;

import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_BATCH_PROCESS;

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

    /** Running query manager. */
    private final RunningQueryManager runningQryMgr;

    /** Tracing processor. */
    private final Tracing tracing;

    /** Query id. */
    private final Long qryId;

    /** Exception, current load process ended with, or {@code null} if in progress or if succeded. */
    private Throwable failReason;

    /** Span of the running query. */
    private final Span qrySpan;

    private long updateCnt;

    /** Tracing processor. */

    /** Span of the running query. */

    public BulkLoadProcessor(IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> dataConverter,
                             BulkLoadCacheWriter outputStreamer, RunningQueryManager runningQryMgr,Long qryId,
                             Tracing tracing) {
        this.dataConverter = dataConverter;
        this.outputStreamer = outputStreamer;
        this.updateCnt = 0;
        this.runningQryMgr = runningQryMgr;
        this.qryId = qryId;
        this.tracing = tracing;

        GridRunningQueryInfo qryInfo = runningQryMgr.runningQueryInfo(qryId);

        qrySpan = qryInfo == null ? NoopSpan.INSTANCE : qryInfo.span();
    }

    /**
     * Load read data ito GridGain cluster
     * @param batch
     */
    public void processBatch(List<List<String>> batch) {
        try (MTC.TraceSurroundings ignored = MTC.support(tracing.create(SQL_BATCH_PROCESS, qrySpan))) {
            for (List<String> record : batch) {
                IgniteBiTuple<?, ?> kv = dataConverter.apply(record);
                outputStreamer.apply(kv);
                this.updateCnt++;
            }
            outputStreamer.flush();
        } catch (Exception e) {
            failReason = e;
            throw e;
        } finally {
                runningQryMgr.unregister(qryId, failReason);
        }
    }

    /**
     * @return number of inserted rows
     */
    public long getUpdateCnt() {
        return this.updateCnt;
    }
}

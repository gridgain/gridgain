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

package org.apache.ignite.internal.processors.query.h2;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;

public class QueryMemoryManager extends H2MemoryTracker {
    //TODO: GG-18629: Move defaults to memory quotas configuration.
    /**
     * Default query memory limit.
     *
     * Note: Actually, it is  per query (Map\Reduce) stage limit. With QueryParallelism every query-thread will be
     * treated as separate Map query.
     */
    private final long dfltSqlQryMemoryLimit;

    /** Global query memory quota. */
    //TODO GG-18628: it looks safe to make this configurable at runtime.
    private final long globalQuota;

    /** Memory allocated by running queries. */
    private AtomicLong allocated = new AtomicLong();

    /**
     * @param globalQuota Node memory available for sql queries.
     */
    public QueryMemoryManager(long globalQuota) {
        //TODO GG-18628: Add check if Heap has enough free memory.
        assert Runtime.getRuntime().maxMemory() > globalQuota;

        if (globalQuota == 0) {
            globalQuota = Long.getLong(IgniteSystemProperties.IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE,
                (long)(Runtime.getRuntime().maxMemory() * 0.6d));
        }

        dfltSqlQryMemoryLimit = Long.getLong(IgniteSystemProperties.IGNITE_DEFAULT_SQL_QUERY_MEMORY_LIMIT,
        globalQuota / IgniteConfiguration.DFLT_QUERY_THREAD_POOL_SIZE);

        this.globalQuota = globalQuota;
    }

    /** {@inheritDoc} */
    @Override public void allocate(long size) {
        assert size >= 0;

        if (size == 0)
            return; // Nothing to do.

        allocated.accumulateAndGet(size, (prev, x) -> {
            if (prev + x > globalQuota)
                throw new IgniteSQLException("SQL query run out of memory: Global quota exceeded. "+x,
                    IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY);

            return prev + x;
        });
    }

    /** {@inheritDoc} */
    @Override public void release(long size) {
        assert size >= 0;

        if (size == 0)
            return; // Nothing to do.

        allocated.accumulateAndGet(-size, (prev, x) -> {
            if (prev + x < 0)
                throw new IllegalStateException("Try to free more memory that ever be allocated: [" +
                    "allocated=" + prev + ", toFree=" + x + ']');

            return prev + x;
        });
    }

    /**
     * Query memory tracker factory method.
     * Note: If 'maxQueryMemory' is zero, then {@link QueryMemoryManager#dfltSqlQryMemoryLimit}  will be used.
     * Note: Negative values are reserved for disable memory tracking.
     *
     * @param maxQueryMemory Query memory limit in bytes.
     * @return Query memory tracker.
     */
    public QueryMemoryTracker createQueryMemoryTracker(long maxQueryMemory) {
        assert globalQuota == 0 || globalQuota > maxQueryMemory : globalQuota;

        //TODO: GG-18628: Should we register newly created tracker? This can be helpful in debugging 'memory leaks'.
        return new QueryMemoryTracker(this, maxQueryMemory > 0 ? maxQueryMemory : dfltSqlQryMemoryLimit);
    }

    /**
     * Gets memory allocated by running queries.
     *
     * @return Allocated memory in bytes.
     */
    public long allocated() {
        return allocated.get();
    }

    /**
     * Gets memory available for queries.
     *
     * @return Available memory in bytes.
     */
    public long free() {
        return globalQuota - allocated.get();
    }

    /**
     * Gets global memory limit for queries.
     *
     * @return Max memory in bytes.
     */
    public long maxMemory() {
        return globalQuota;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }
}

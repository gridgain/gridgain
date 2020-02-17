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
import java.util.function.LongBinaryOperator;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.metric.SqlStatisticsHolderMemoryQuotas;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.util.IgniteUtils.KB;

/**
 * Query memory manager.
 */
public class QueryMemoryManager implements H2MemoryTracker {
    /**
     * Default memory reservation block size.
     */
    public static final long DFLT_MEMORY_RESERVATION_BLOCK_SIZE = 512 * KB;

    /** Set of metrics that collect info about memory this memory manager tracks. */
    private final SqlStatisticsHolderMemoryQuotas metrics;

    /** */
    static final LongBinaryOperator RELEASE_OP = new LongBinaryOperator() {
        @Override public long applyAsLong(long prev, long x) {
            long res = prev - x;

            if (res < 0)
                throw new IllegalStateException("Try to free more memory that ever be reserved: [" +
                    "reserved=" + prev + ", toFree=" + x + ']');

            return res;
        }
    };

    /** Logger. */
    private final IgniteLogger log;

    /** Global memory quota. */
    private volatile long globalQuota;

    /** String representation of global quota. */
    private String globalQuotaStr;

    /**
     * Default query memory limit.
     *
     * Note: Actually, it is  per query (Map\Reduce) stage limit. With QueryParallelism every query-thread will be
     * treated as separate Map query.
     */
    private volatile long qryQuota;

    /** String representation of query quota. */
    private String qryQuotaStr;

    /** Reservation block size. */
    private final long blockSize;

    /**
     * Defines an action that occurs when the memory limit is exceeded. Possible variants:
     * <ul>
     * <li>{@code false} - exception will be thrown.</li>
     * <li>{@code true} - intermediate query results will be spilled to the disk.</li>
     * </ul>
     */
    private volatile boolean offloadingEnabled;

    /** Memory reserved by running queries. */
    private final AtomicLong reserved = new AtomicLong();

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public QueryMemoryManager(GridKernalContext ctx) {
        this.log = ctx.log(QueryMemoryManager.class);

        setGlobalQuota(ctx.config().getSqlGlobalMemoryQuota());

        if (Runtime.getRuntime().maxMemory() <= globalQuota)
            throw new IllegalStateException("Sql memory pool size can't be more than heap memory max size.");


        setQueryQuota(ctx.config().getSqlQueryMemoryQuota());
        this.offloadingEnabled = ctx.config().isSqlOffloadingEnabled();
        this.metrics = new SqlStatisticsHolderMemoryQuotas(this, ctx.metric());
        this.blockSize = Long.getLong(IgniteSystemProperties.IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE,
            DFLT_MEMORY_RESERVATION_BLOCK_SIZE);


        A.ensure(globalQuota >= 0, "Sql global memory quota must be >= 0. But was " + globalQuota);
        A.ensure(qryQuota >= 0, "Sql query memory quota must be >= 0. But was " + qryQuota);
        A.ensure(blockSize > 0, "Block size must be > 0. But was " + blockSize);
    }

    /** {@inheritDoc} */
    @Override public boolean reserved(long size) {
        if (size == 0)
            return true; // Nothing to do.

        long reserved0 = reserved.addAndGet(size);

        if (reserved0 >= globalQuota)
            return onQuotaExceeded(size);

        metrics.trackReserve(size);

        return true;
    }

    /** {@inheritDoc} */
    @Override public void released(long size) {
        assert size >= 0;

        if (size == 0)
            return; // Nothing to do.

        assert size > 0;

        reserved.accumulateAndGet(size, RELEASE_OP);
    }

    /**
     * Query memory tracker factory method.
     *
     * Note: If 'maxQueryMemory' is zero, then {@link QueryMemoryManager#qryQuota}  will be used.
     * Note: Negative values are reserved for disable memory tracking.
     *
     * @param maxQueryMemory Query memory limit in bytes.
     * @return Query memory tracker.
     */
    public QueryMemoryTracker createQueryMemoryTracker(long maxQueryMemory) {
        assert maxQueryMemory >= 0;

        if (maxQueryMemory == 0)
            maxQueryMemory = qryQuota;

        long globalQuota0 = globalQuota;

        if (maxQueryMemory == 0 && globalQuota0 == 0)
            return null; // No memory tracking configured.

        if (globalQuota0 > 0 && globalQuota0 < maxQueryMemory) {
            U.warn(log, "Max query memory can't exceed SQL memory pool size. Will be reduced down to: " + globalQuota);

            maxQueryMemory = globalQuota0;
        }

        H2MemoryTracker parent = globalQuota0 == 0 ? null : this;

        return new QueryMemoryTracker(parent, maxQueryMemory, blockSize, offloadingEnabled);
    }

    /**
     * Action when quota is exceeded.
     * @return {@code false} if it is needed to offload data.
     */
    public boolean onQuotaExceeded(long size) {
        reserved.addAndGet(-size);

        if (offloadingEnabled)
            return false;
        else {
            throw new IgniteSQLException("SQL query run out of memory: Global quota exceeded.",
                IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY);
        }
    }

    /**
     * Sets new global query quota.
     *
     * @param newGlobalQuota New global query quota.
     */
    public void setGlobalQuota(String newGlobalQuota) {
        this.globalQuota = U.parseBytes(newGlobalQuota);
        this.globalQuotaStr = newGlobalQuota;
    }

    /**
     * @return Current global query quota.
     */
    public String getGlobalQuota() {
        return globalQuotaStr;
    }

    /**
     * Sets new per-query quota.
     *
     * @param newQryQuota New per-query quota.
     */
    public void setQueryQuota(String newQryQuota) {
        this.qryQuota = U.parseBytes(newQryQuota);
        this.qryQuotaStr = newQryQuota;
    }

    /**
     * @return Current query quota.
     */
    public String getQueryQuotaString() {
        return qryQuotaStr;
    }

    /**
     * Sets offloading enabled flag.
     *
     * @param offloadingEnabled Offloading enabled flag.
     */
    public void setOffloadingEnabled(boolean offloadingEnabled) {
        this.offloadingEnabled = offloadingEnabled;
    }

    /**
     * @return Flag whether offloading is enabled.
     */
    public boolean isOffloadingEnabled() {
        return offloadingEnabled;
    }

    /** {@inheritDoc} */
    @Override public long memoryReserved() {
        return reserved.get();
    }

    /** {@inheritDoc} */
    @Override public long memoryLimit() {
        return globalQuota;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Cursors are not tracked and can't be forcibly closed to release resources.
        // For now, it is ok as neither extra memory is actually hold with MemoryManager nor file descriptors are used.
        if (log.isDebugEnabled() && reserved.get() != 0)
            log.debug("Potential memory leak in SQL processor. Some query cursors were not closed or forget to free memory.");
    }
}

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

import java.io.File;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.exceptions.SqlMemoryQuotaExceededException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.metric.SqlMemoryStatisticsHolder;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.query.GridQueryMemoryMetricProvider;
import org.apache.ignite.internal.processors.query.h2.disk.ExternalResultData;
import org.apache.ignite.internal.processors.query.h2.disk.GroupedExternalResult;
import org.apache.ignite.internal.processors.query.h2.disk.PlainExternalResult;
import org.apache.ignite.internal.processors.query.h2.disk.SortedExternalResult;
import org.apache.ignite.internal.processors.query.h2.disk.TrackableFileIoFactory;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.command.dml.GroupByData;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.result.ResultExternal;
import org.h2.result.SortOrder;

import static org.apache.ignite.internal.util.IgniteUtils.KB;

/**
 * Query memory manager.
 */
public class QueryMemoryManager implements H2MemoryTracker, ManagedGroupByDataFactory {
    /**
     *  Spill directory path. Spill directory is used for the disk offloading
     *  of intermediate results of the heavy queries.
     */
    public static final String DISK_SPILL_DIR = "tmp/spill";

    /** */
    private final GridKernalContext ctx;

    /**
     * Default memory reservation block size.
     */
    public static final long DFLT_MEMORY_RESERVATION_BLOCK_SIZE = 512 * KB;

    /** Set of metrics that collect info about memory this memory manager tracks. */
    private final SqlMemoryStatisticsHolder metrics;

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

    /**
     * Default query memory limit.
     *
     * Note: Actually, it is  per query (Map\Reduce) stage limit. With QueryParallelism every query-thread will be
     * treated as separate Map query.
     */
    private volatile long qryQuota;

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

    /** Factory to provide I/O interface for data storage files */
    private final TrackableFileIoFactory fileIOFactory;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public QueryMemoryManager(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(QueryMemoryManager.class);

        setGlobalQuota(ctx.config().getSqlConfiguration().getSqlGlobalMemoryQuota());
        setQueryQuota(ctx.config().getSqlConfiguration().getSqlQueryMemoryQuota());

        offloadingEnabled = ctx.config().getSqlConfiguration().isSqlOffloadingEnabled();
        metrics = new SqlMemoryStatisticsHolder(this, ctx.metric());
        blockSize = Long.getLong(IgniteSystemProperties.IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE,
            DFLT_MEMORY_RESERVATION_BLOCK_SIZE);

        final FileIOFactory delegateFactory =
            IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_USE_ASYNC_FILE_IO_FACTORY, true) ?
            new AsyncFileIOFactory() : new RandomAccessFileIOFactory();

        fileIOFactory = new TrackableFileIoFactory(delegateFactory, metrics);

        A.ensure(blockSize > 0, "Block size must be > 0: blockSize=" + blockSize);
    }

    /** {@inheritDoc} */
    @Override public boolean reserve(long size) {
        if (size == 0)
            return true; // Nothing to do.

        long reserved0 = reserved.addAndGet(size);

        if (globalQuota > 0 && reserved0 >= globalQuota)
            return onQuotaExceeded(size);

        metrics.trackReserve();

        return true;
    }

    /** {@inheritDoc} */
    @Override public void release(long size) {
        assert size >= 0;

        if (size == 0)
            return; // Nothing to do.

        assert size > 0;

        reserved.accumulateAndGet(size, RELEASE_OP);
    }

    /**
     * Query memory tracker factory method.
     *
     * Note: If 'maxQueryMemory' is zero, then {@link QueryMemoryManager#qryQuota} will be used.
     *
     * @param maxQryMemory Query memory limit in bytes.
     * @return Query memory tracker.
     */
    public GridQueryMemoryMetricProvider createQueryMemoryTracker(long maxQryMemory) {
        long globalQuota0 = globalQuota;

        if (globalQuota0 > 0 && globalQuota0 < maxQryMemory) {
            if (log.isInfoEnabled()) {
                LT.info(log, "Query memory quota cannot exceed global memory quota." +
                    " It will be reduced to the size of global quota: " + globalQuota0);
            }

            maxQryMemory = globalQuota0;
        }

        if (maxQryMemory == 0)
            maxQryMemory = globalQuota0 > 0 ? Math.min(qryQuota, globalQuota0) : qryQuota;

        if (maxQryMemory < 0)
            maxQryMemory = 0;

        QueryMemoryTracker tracker = new QueryMemoryTracker(this, maxQryMemory, blockSize, offloadingEnabled);

        if (log.isDebugEnabled())
            log.debug("Memory tracker created: " + tracker);

        return tracker;
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
            throw new SqlMemoryQuotaExceededException("SQL query ran out of memory: Global quota was exceeded.");
        }
    }

    /**
     * Sets new global query quota.
     *
     * @param newGlobalQuota New global query quota.
     */
    public synchronized void setGlobalQuota(String newGlobalQuota) {
        long globalQuota0 = U.parseBytes(newGlobalQuota);
        long heapSize = Runtime.getRuntime().maxMemory();

        A.ensure(
            heapSize > globalQuota0,
            "Sql global memory quota can't be more than heap size: heapSize="
                + heapSize + ", quotaSize=" + globalQuota0
        );

        A.ensure(globalQuota0 >= 0, "Sql global memory quota must be >= 0: quotaSize=" + globalQuota0);

        globalQuota = globalQuota0;

        if (log.isInfoEnabled()) {
            log.info("SQL query global quota was set to " + globalQuota + ". Current memory tracking parameters: " +
                "[qryQuota=" + qryQuota + ", globalQuota=" + globalQuota +
                ", offloadingEnabled=" + offloadingEnabled + ']');
        }
    }

    /**
     * @return Current global query quota.
     */
    public String getGlobalQuota() {
        return String.valueOf(globalQuota);
    }

    /**
     * Sets new per-query quota.
     *
     * @param newQryQuota New per-query quota.
     */
    public synchronized void setQueryQuota(String newQryQuota) {
        long qryQuota0 = U.parseBytes(newQryQuota);

        A.ensure(qryQuota0 >= 0, "Sql query memory quota must be >= 0: quotaSize=" + qryQuota0);

        qryQuota = U.parseBytes(newQryQuota);

        if (log.isInfoEnabled()) {
            log.info("SQL query memory quota was set to " + qryQuota + ". Current memory tracking parameters: " +
                "[qryQuota=" + qryQuota + ", globalQuota=" + globalQuota +
                ", offloadingEnabled=" + offloadingEnabled + ']');
        }

        if (qryQuota > globalQuota) {
            log.warning("The local quota was set higher than global. The new value will be truncated " +
                "to the size of the global quota [qryQuota=" + qryQuota + ", globalQuota=" + globalQuota);
        }
    }

    /**
     * @return Current query quota.
     */
    public String getQueryQuotaString() {
        return String.valueOf(qryQuota);
    }

    /**
     * Sets offloading enabled flag.
     *
     * @param offloadingEnabled Offloading enabled flag.
     */
    public synchronized void setOffloadingEnabled(boolean offloadingEnabled) {
        this.offloadingEnabled = offloadingEnabled;

        if (log.isInfoEnabled()) {
            log.info("SQL query query offloading enabled flag was set to " + offloadingEnabled +
                ". Current memory tracking parameters: [qryQuota=" + qryQuota + ", globalQuota=" + globalQuota +
                ", offloadingEnabled=" + this.offloadingEnabled + ']');
        }
    }

    /**
     * @return Flag whether offloading is enabled.
     */
    public boolean isOffloadingEnabled() {
        return offloadingEnabled;
    }

    /**
     * @return Bytes reserved by all queries.
     */
    @Override public long reserved() {
        return reserved.get();
    }

    /** */
    public long memoryLimit() {
        return globalQuota;
    }

    /** {@inheritDoc} */
    @Override public void spill(long size) {
        // NO-OP
    }

    /** {@inheritDoc} */
    @Override public void unspill(long size) {
        // NO-OP
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Cursors are not tracked and can't be forcibly closed to release resources.
        // For now, it is ok as neither extra memory is actually hold with MemoryManager nor file descriptors are used.
        if (log.isDebugEnabled() && reserved.get() != 0)
            log.debug("Potential memory leak in SQL processor. Some query cursors were not closed or forget to free memory.");
    }

    /** {@inheritDoc} */
    @Override public void incrementFilesCreated() {
        // NO-OP
    }

    /** {@inheritDoc} */
    @Override public H2MemoryTracker createChildTracker() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long writtenOnDisk() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long totalWrittenOnDisk() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void onChildClosed(H2MemoryTracker child) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean closed() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return Global quota.
     */
    public long globalQuota() {
        return globalQuota;
    }

    /** */
    public IgniteLogger log() {
        return log;
    }

    /**
     * Cleans spill directory. Spill directory is used for disk
     * offloading of the intermediate results of heavy queries.
     */
    public void cleanSpillDirectory() {
        try {
            File spillDir = U.resolveWorkDirectory(
                ctx.config().getWorkDirectory(),
                DISK_SPILL_DIR,
                false);

            File[] spillFiles = spillDir.listFiles();

            if (spillFiles.length == 0)
                return;

            for (int i = 0; i < spillFiles.length; i++) {
                try {
                    File spillFile = spillFiles[i];

                    String nodeId = spillFile.getName().split("_")[1]; // Spill name pattern: spill_nodeId_fileId.

                    UUID nodeUuid = UUID.fromString(nodeId);

                    if (!ctx.discovery().alive(nodeUuid) || ctx.localNodeId().equals(nodeUuid))
                        spillFile.delete();
                }
                catch (Exception e) {
                    log.debug("Error on cleaning spill directory. " + X.getFullStackTrace(e));
                }
            }
        }
        catch (Exception e) {
            log.warning("Failed to cleanup the temporary directory for intermediate " +
                "SQL query results from the previous node run.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public GroupByData newManagedGroupByData(Session ses, ArrayList<Expression> expressions,
        boolean isGrpQry, int[] grpIdx) {

        boolean spillingEnabled = ctx.config().getSqlConfiguration().isSqlOffloadingEnabled();

        if (!spillingEnabled)
            return null;

        assert isGrpQry; // isGrpQry == false allowed only for window queries which are not supported yet.

        return new H2ManagedGroupByData(ses, grpIdx);
    }

    /**
     * @param ses Session.
     * @return Plain external result.
     */
    public ResultExternal createPlainExternalResult(Session ses) {
        return new PlainExternalResult(ses);
    }

    /**
     * @param ses Session.
     * @param distinct Distinct flag.
     * @param distinctIndexes Distinct indexes.
     * @param visibleColCnt Visible columns count.
     * @param sort Sort order.
     * @param rowCnt Row count.
     * @return Sorted external result.
     */
    public ResultExternal createSortedExternalResult(Session ses, boolean distinct, int[] distinctIndexes,
        int visibleColCnt, SortOrder sort, int rowCnt) {
        return new SortedExternalResult(ses, distinct, distinctIndexes, visibleColCnt, sort, rowCnt);
    }

    /**
     * @param ses Session.
     * @param size Size;
     * @return Grouped result;
     */
    public GroupedExternalResult createGroupedExternalResult(Session ses, int size) {
        return new GroupedExternalResult(ses, size);
    }

    /**
     * Creates external data (offload file wrapper).
     * @param ses Session.
     * @param useHashIdx Flag whether to use hash index.
     * @param initSize Initial size.
     * @param cls Class of stored values.
     * @param <T> Type of stored values.
     * @return Created external data (offload file wrapper).
     */
    public <T> ExternalResultData<T> createExternalData(Session ses, boolean useHashIdx, long initSize, Class<T> cls) {
        H2MemoryTracker tracker = ses.memoryTracker();

        if (tracker.totalWrittenOnDisk() == 0) {
            metrics.trackQueryOffloaded();

            if (log.isInfoEnabled())
                LT.info(log, "Offloading started for query: " + ses.queryDescription());
        }

        return new ExternalResultData<T>(log,
            ctx.config().getWorkDirectory(),
            fileIOFactory,
            ctx.localNodeId(),
            useHashIdx,
            initSize,
            cls,
            ses.getDatabase().getCompareMode(),
            ses.getDatabase(),
            ses.memoryTracker());
    }
}

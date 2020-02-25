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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.metric.SqlMemoryStatisticsHolder;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.disk.ExternalResultData;
import org.apache.ignite.internal.processors.query.h2.disk.GroupedExternalResult;
import org.apache.ignite.internal.processors.query.h2.disk.PlainExternalResult;
import org.apache.ignite.internal.processors.query.h2.disk.SortedExternalResult;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
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

    /** Factory to provide I/O interface for data storage files */
    private final FileIOFactory fileIOFactory;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public QueryMemoryManager(GridKernalContext ctx) {
        this.log = ctx.log(QueryMemoryManager.class);
        this.ctx = ctx;
        if (Runtime.getRuntime().maxMemory() <= globalQuota)
            throw new IllegalStateException("Sql memory pool size can't be more than heap memory max size.");

        setGlobalQuota(ctx.config().getSqlGlobalMemoryQuota());
        setQueryQuota(ctx.config().getSqlQueryMemoryQuota());
        this.offloadingEnabled = ctx.config().isSqlOffloadingEnabled();
        this.metrics = new SqlMemoryStatisticsHolder(this, ctx.metric());
        this.blockSize = Long.getLong(IgniteSystemProperties.IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE,
            DFLT_MEMORY_RESERVATION_BLOCK_SIZE);

        final FileIOFactory delegateFactory =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_USE_ASYNC_FILE_IO_FACTORY, true) ?
            new AsyncFileIOFactory() : new RandomAccessFileIOFactory();

        fileIOFactory = new FileIOFactory() {
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                FileIO delegate = delegateFactory.create(file, modes);

                return new TrackableFileIO(delegate, metrics);
            }
        };

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

        metrics.trackReserve();

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

        boolean spillingEnabled = ctx.config().isSqlOffloadingEnabled();

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
        if (!ses.isOffloadedToDisk()) {
            ses.setOffloadedToDisk(true);

            metrics.trackQueryOffloaded();
        }

        return new ExternalResultData<>(log,
            ctx.config().getWorkDirectory(),
            fileIOFactory,
            ctx.localNodeId(),
            useHashIdx,
            initSize,
            cls,
            ses.getDatabase().getCompareMode(),
            ses.getDatabase());
    }

    /**
     * FileIO decorator for stats collecting.
     */
    private static class TrackableFileIO extends FileIODecorator {
        /** */
        private final SqlMemoryStatisticsHolder metrics;

        /**
         * @param delegate File I/O delegate
         */
        private TrackableFileIO(FileIO delegate, SqlMemoryStatisticsHolder metrics) {
            super(delegate);

            this.metrics = metrics;
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer destBuf) throws IOException {
            int bytesRead = delegate.read(destBuf);

            if (bytesRead > 0)
                metrics.trackOffloadingRead(bytesRead);

            return bytesRead;
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer destBuf, long position) throws IOException {
            int bytesRead = delegate.read(destBuf, position);

            if (bytesRead > 0)
                metrics.trackOffloadingRead(bytesRead);

            return bytesRead;
        }

        /** {@inheritDoc} */
        @Override public int read(byte[] buf, int off, int len) throws IOException {
            int bytesRead = delegate.read(buf, off, len);

            if (bytesRead > 0)
                metrics.trackOffloadingRead(bytesRead);

            return bytesRead;
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf) throws IOException {
            int bytesWritten = delegate.write(srcBuf);

            if (bytesWritten > 0)
                metrics.trackOffloadingWritten(bytesWritten);

            return bytesWritten;
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
            int bytesWritten = delegate.write(srcBuf, position);

            if (bytesWritten > 0)
                metrics.trackOffloadingWritten(bytesWritten);

            return bytesWritten;
        }

        /** {@inheritDoc} */
        @Override public int write(byte[] buf, int off, int len) throws IOException {
            int bytesWritten = delegate.write(buf, off, len);

            if (bytesWritten > 0)
                metrics.trackOffloadingWritten(bytesWritten);

            return bytesWritten;
        }
    }
}

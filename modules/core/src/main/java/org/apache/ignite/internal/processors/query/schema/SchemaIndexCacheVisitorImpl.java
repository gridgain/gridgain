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

package org.apache.ignite.internal.processors.query.schema;

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING;
import static org.apache.ignite.IgniteSystemProperties.INDEX_REBUILDING_PARALLELISM;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;

/**
 * Traversor operating all primary and backup partitions of given cache.
 */
public class SchemaIndexCacheVisitorImpl implements SchemaIndexCacheVisitor {
    /** Default degree of parallelism. */
    private static final int DFLT_PARALLELISM;

    /** Count of rows, being processed within a single checkpoint lock. */
    private static final int BATCH_SIZE = 1000;

    /** Is extra index rebuild logging enabled. */
    private final boolean collectStat = getBoolean(IGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING, false);

    /** Cache context. */
    private final GridCacheContext cctx;

    /** Cancellation token. */
    private final SchemaIndexOperationCancellationToken cancel;

    /** Parallelism. */
    private final int parallelism;

    /** Whether to stop the process. */
    private volatile boolean stop;

    /** Logger. */
    private IgniteLogger log;

    static {
        int parallelism = IgniteSystemProperties.getInteger(INDEX_REBUILDING_PARALLELISM, 0);

        if (parallelism > 0)
            DFLT_PARALLELISM = Math.min(parallelism, Runtime.getRuntime().availableProcessors());
        else
            DFLT_PARALLELISM = Math.min(4, Math.max(1, Runtime.getRuntime().availableProcessors() / 4));
    }

    /**
     * Constructor.
     *  @param cctx Cache context.
     */
    public SchemaIndexCacheVisitorImpl(GridCacheContext cctx) {
        this(cctx, null, 0);
    }

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param cancel Cancellation token.
     * @param parallelism Degree of parallelism.
     */
    public SchemaIndexCacheVisitorImpl(GridCacheContext cctx, SchemaIndexOperationCancellationToken cancel,
        int parallelism) {
        this.cancel = cancel;

        if (parallelism > 0)
            this.parallelism = Math.min(Runtime.getRuntime().availableProcessors(), parallelism);
        else
            this.parallelism = DFLT_PARALLELISM;

        if (cctx.isNear())
            cctx = ((GridNearCacheAdapter)cctx.cache()).dht().context();

        this.cctx = cctx;

        this.log = cctx.logger(SchemaIndexCacheVisitorImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void visit(SchemaIndexCacheVisitorClosure clo) throws IgniteCheckedException {
        assert clo != null;

        List<GridDhtLocalPartition> parts = cctx.topology().localPartitions();

        if (parts.isEmpty())
            return;

        GridCompoundFuture<SchemaIndexCacheStat, SchemaIndexCacheStat> fut = null;

        if (parallelism > 1) {
            fut = new GridCompoundFuture<>(new SchemaIndexCacheStatFutureReducer());

            for (int i = 1; i < parallelism; i++)
                fut.add(processPartitionsAsync(parts, clo, i));

            fut.markInitialized();
        }

        final SchemaIndexCacheStat stat = processPartitions(parts, clo, 0);

        if (fut != null && stat != null) {
            final SchemaIndexCacheStat st = fut.get();
            
            stat.scanned += st.scanned;
            stat.types.putAll(st.types);
        }

        printIndexStats(stat);
    }

    /**
     * Prints index cache stats to log.
     *
     * @param stat Index cache stats.
     * @throws IgniteCheckedException if failed to get index size.
     */
    private void printIndexStats(@Nullable SchemaIndexCacheStat stat) throws IgniteCheckedException {
        if (stat == null)
            return;

        SB res = new SB();

        res.a("Details for cache rebuilding [name=" + cctx.cache().name() + ", grpName=" + cctx.group().name() + ']');
        res.a(U.nl());
        res.a("   Scanned rows " + stat.scanned + ", visited types " + stat.types.keySet());
        res.a(U.nl());

        final GridQueryIndexing idx = cctx.kernalContext().query().getIndexing();

        for (QueryTypeDescriptorImpl type : stat.types.values()) {
            res.a("        Type name=" + type.name());
            res.a(U.nl());

            final String pk = "_key_PK";

            res.a("            Index: name=" + pk + ", size=" + idx.indexSize(type.schemaName(), pk));
            res.a(U.nl());

            final Map<String, GridQueryIndexDescriptor> indexes = type.indexes();

            for (GridQueryIndexDescriptor descriptor : indexes.values()) {
                final long size = idx.indexSize(type.schemaName(), descriptor.name());

                res.a("            Index: name=" + descriptor.name() + ", size=" + size);
                res.a(U.nl());
            }
        }

        log.info(res.toString());
    }

    /**
     * Process partitions asynchronously.
     *
     * @param parts Partitions.
     * @param clo Closure.
     * @param remainder Remainder.
     * @return Future.
     */
    private GridFutureAdapter<SchemaIndexCacheStat> processPartitionsAsync(List<GridDhtLocalPartition> parts,
        SchemaIndexCacheVisitorClosure clo, int remainder) {
        GridFutureAdapter<SchemaIndexCacheStat> fut = new GridFutureAdapter<>();

        AsyncWorker worker = new AsyncWorker(parts, clo, remainder, fut);

        new IgniteThread(worker).start();

        return fut;
    }

    /**
     * Process partitions.
     *
     * @param parts Partitions.
     * @param clo Closure.
     * @param remainder Remainder.
     * @return Index rebuild statistics or {@code null}, if
     * {@link IgniteSystemProperties#IGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING} is {@code false}.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private SchemaIndexCacheStat processPartitions(List<GridDhtLocalPartition> parts, SchemaIndexCacheVisitorClosure clo,
        int remainder)
        throws IgniteCheckedException {

        SchemaIndexCacheStat stat = collectStat ? new SchemaIndexCacheStat() : null;

        for (int i = 0, size = parts.size(); i < size; i++) {
            if (stop)
                break;

            if ((i % parallelism) == remainder)
                processPartition(parts.get(i), clo, stat);
        }

        return stat;
    }

    /**
     * Process partition.
     *
     * @param part Partition.
     * @param clo Index closure.
     * @param stat Index build statistics accumulator (can be {@code null}).
     * @throws IgniteCheckedException If failed.
     */
    private void processPartition(
        GridDhtLocalPartition part,
        SchemaIndexCacheVisitorClosure clo,
        @Nullable SchemaIndexCacheStat stat
    ) throws IgniteCheckedException {
        checkCancelled();

        boolean reserved = false;

        if (part != null && part.state() != EVICTED)
            reserved = (part.state() == OWNING || part.state() == RENTING || part.state() == MOVING) && part.reserve();

        if (!reserved)
            return;

        try {
            GridCursor<? extends CacheDataRow> cursor = part.dataStore().cursor(cctx.cacheId(), null, null,
                CacheDataRowAdapter.RowData.KEY_ONLY);

            boolean locked = false;

            try {
                int cntr = 0;

                while (cursor.next() && !stop) {
                    KeyCacheObject key = cursor.get().key();

                    if (!locked) {
                        cctx.shared().database().checkpointReadLock();

                        locked = true;
                    }

                    processKey(key, clo, stat);

                    if (++cntr % BATCH_SIZE == 0) {
                        cctx.shared().database().checkpointReadUnlock();

                        locked = false;
                    }

                    if (part.state() == RENTING)
                        break;
                }

                if (stat != null)
                    stat.scanned += cntr;
            }
            finally {
                if (locked)
                    cctx.shared().database().checkpointReadUnlock();
            }
        }
        finally {
            part.release();

            cctx.group().metrics().decrementIndexBuildCountPartitionsLeft();
        }
    }

    /**
     * Process single key.
     *
     * @param key Key.
     * @param clo Closure.
     * @param stat Index build statistics accumulator (can be {@code null}).
     * @throws IgniteCheckedException If failed.
     */
    private void processKey(
        KeyCacheObject key,
        SchemaIndexCacheVisitorClosure clo,
        @Nullable SchemaIndexCacheStat stat
    ) throws IgniteCheckedException {
        while (true) {
            try {
                checkCancelled();

                GridCacheEntryEx entry = cctx.cache().entryEx(key);

                try {
                    entry.updateIndex(clo, stat);
                }
                finally {
                    entry.touch();
                }

                break;
            }
            catch (GridDhtInvalidPartitionException ignore) {
                break;
            }
            catch (GridCacheEntryRemovedException ignored) {
                // No-op.
            }
        }
    }

    /**
     * Check if visit process is not cancelled.
     *
     * @throws IgniteCheckedException If cancelled.
     */
    private void checkCancelled() throws IgniteCheckedException {
        if (cancel != null && cancel.isCancelled())
            throw new IgniteCheckedException("Index creation was cancelled.");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaIndexCacheVisitorImpl.class, this);
    }

    /**
     * Async worker.
     */
    private class AsyncWorker extends GridWorker {
        /** Partitions. */
        private final List<GridDhtLocalPartition> parts;

        /** Closure. */
        private final SchemaIndexCacheVisitorClosure clo;

        /** Remained.. */
        private final int remainder;

        /** Future. */
        private final GridFutureAdapter<SchemaIndexCacheStat> fut;

        /**
         * Constructor.
         *
         * @param parts Partitions.
         * @param clo Closure.
         * @param remainder Remainder.
         * @param fut Future.
         */
        @SuppressWarnings("unchecked")
        public AsyncWorker(
            List<GridDhtLocalPartition> parts,
            SchemaIndexCacheVisitorClosure clo,
            int remainder,
            GridFutureAdapter<SchemaIndexCacheStat> fut
        ) {
            super(cctx.igniteInstanceName(), "parallel-idx-worker-" + cctx.cache().name() + "-" + remainder,
                cctx.logger(AsyncWorker.class));

            this.parts = parts;
            this.clo = clo;
            this.remainder = remainder;
            this.fut = fut;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                processPartitions(parts, clo, remainder);
            }
            catch (Throwable e) {
                err = e;

                U.error(log, "Error during parallel index create/rebuild.", e);

                stop = true;

                cctx.group().metrics().setIndexBuildCountPartitionsLeft(0);
            }
            finally {
                fut.onDone(err);
            }
        }
    }

    /**
     * Reducer for parallel index rebuild.
     */
    private static class SchemaIndexCacheStatFutureReducer implements IgniteReducer<SchemaIndexCacheStat, SchemaIndexCacheStat> {
        /** */
        private static final long serialVersionUID = 0L;

        /**  */
        private final SchemaIndexCacheStat res = new SchemaIndexCacheStat();

        /** {@inheritDoc} */
        @Override public boolean collect(SchemaIndexCacheStat stat) {
            if (stat != null) {
                synchronized (res) {
                    res.scanned += stat.scanned;
                    res.types.putAll(stat.types);
                }
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public SchemaIndexCacheStat reduce() {
            synchronized (res) {
                return res;
            }
        }
    }
}

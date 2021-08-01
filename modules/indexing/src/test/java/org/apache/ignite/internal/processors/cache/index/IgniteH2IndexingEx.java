/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.index;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexOperationCancellationToken;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyMap;
import static org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.DO_NOTHING;
import static org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.DO_NOTHING_CACHE_DATA_ROW_CONSUMER;
import static org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.nodeName;

/**
 * Extension {@link IgniteH2Indexing} for the test.
 */
class IgniteH2IndexingEx extends IgniteH2Indexing {
    /**
     * Consumer for cache rows when rebuilding indexes on a node.
     * Mapping: Node name -> Cache name -> Consumer.
     */
    private static final Map<String, Map<String, IgniteThrowableConsumer<CacheDataRow>>> cacheRowConsumer =
        new ConcurrentHashMap<>();

    /**
     * A function that should run before preparing to rebuild the cache indexes on a node.
     * Mapping: Node name -> Cache name -> Function.
     */
    private static final Map<String, Map<String, Runnable>> cacheRebuildRunner = new ConcurrentHashMap<>();

    /**
     * Consumer for cache rows when creating an index on a node.
     * Mapping: Node name -> Index name -> Consumer.
     */
    private static final Map<String, Map<String, IgniteThrowableConsumer<CacheDataRow>>> idxCreateCacheRowConsumer =
        new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected void rebuildIndexesFromHash0(
        GridCacheContext cctx,
        SchemaIndexCacheVisitorClosure clo,
        GridFutureAdapter<Void> rebuildIdxFut,
        SchemaIndexOperationCancellationToken cancel
    ) {
        super.rebuildIndexesFromHash0(cctx, new SchemaIndexCacheVisitorClosure() {
            /** {@inheritDoc} */
            @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
                cacheRowConsumer
                    .getOrDefault(nodeName(cctx), emptyMap())
                    .getOrDefault(cctx.name(), DO_NOTHING_CACHE_DATA_ROW_CONSUMER)
                    .accept(row);

                clo.apply(row);
            }
        }, rebuildIdxFut, cancel);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rebuildIndexesFromHash(GridCacheContext cctx, boolean force) {
        cacheRebuildRunner
            .getOrDefault(nodeName(cctx), emptyMap())
            .getOrDefault(cctx.name(), DO_NOTHING)
            .run();

        return super.rebuildIndexesFromHash(cctx, force);
    }

    /** {@inheritDoc} */
    @Override public void dynamicIndexCreate(
        String schemaName,
        String tblName,
        QueryIndexDescriptorImpl idxDesc,
        boolean ifNotExists,
        SchemaIndexCacheVisitor cacheVisitor
    ) throws IgniteCheckedException {
        super.dynamicIndexCreate(schemaName, tblName, idxDesc, ifNotExists, clo -> {
            cacheVisitor.visit(row -> {
                idxCreateCacheRowConsumer
                    .getOrDefault(nodeName(ctx), emptyMap())
                    .getOrDefault(idxDesc.name(), DO_NOTHING_CACHE_DATA_ROW_CONSUMER)
                    .accept(row);

                clo.apply(row);
            });
        });
    }

    /**
     * Cleaning of internal structures. It is recommended to clean at
     * {@code GridCommonAbstractTest#beforeTest} and {@code GridCommonAbstractTest#afterTest}.
     *
     * @param nodeNamePrefix Prefix of node name ({@link GridKernalContext#igniteInstanceName()})
     *      for which internal structures will be removed, if {@code null} will be removed for all.
     *
     * @see GridCommonAbstractTest#getTestIgniteInstanceName()
     */
    static void clean(@Nullable String nodeNamePrefix) {
        if (nodeNamePrefix == null) {
            cacheRowConsumer.clear();
            cacheRebuildRunner.clear();
            idxCreateCacheRowConsumer.clear();
        }
        else {
            cacheRowConsumer.entrySet().removeIf(e -> e.getKey().startsWith(nodeNamePrefix));
            cacheRebuildRunner.entrySet().removeIf(e -> e.getKey().startsWith(nodeNamePrefix));
            idxCreateCacheRowConsumer.entrySet().removeIf(e -> e.getKey().startsWith(nodeNamePrefix));
        }
    }

    /**
     * Set {@link IgniteH2IndexingEx} to {@link GridQueryProcessor#idxCls} before starting the node.
     */
    static void prepareBeforeNodeStart() {
        GridQueryProcessor.idxCls = IgniteH2IndexingEx.class;
    }

    /**
     * Registering a consumer for cache rows when rebuilding indexes on a node.
     *
     * @param nodeName The name of the node,
     *      the value of which will return {@link GridKernalContext#igniteInstanceName()}.
     * @param cacheName Cache name.
     * @param c Cache row consumer.
     *
     * @see IndexingTestUtils#nodeName(GridKernalContext)
     * @see IndexingTestUtils#nodeName(IgniteEx)
     * @see IndexingTestUtils#nodeName(GridCacheContext)
     * @see GridCommonAbstractTest#getTestIgniteInstanceName(int)
     */
    static void addCacheRowConsumer(String nodeName, String cacheName, IgniteThrowableConsumer<CacheDataRow> c) {
        cacheRowConsumer.computeIfAbsent(nodeName, s -> new ConcurrentHashMap<>()).put(cacheName, c);
    }

    /**
     * Registering A function that should run before preparing to rebuild the cache indexes on a node.
     *
     * @param nodeName The name of the node,
     *      the value of which will return {@link GridKernalContext#igniteInstanceName()}.
     * @param cacheName Cache name.
     * @param r A function that should run before preparing to rebuild the cache indexes.
     *
     * @see IndexingTestUtils#nodeName(GridKernalContext)
     * @see IndexingTestUtils#nodeName(IgniteEx)
     * @see IndexingTestUtils#nodeName(GridCacheContext)
     * @see GridCommonAbstractTest#getTestIgniteInstanceName(int)
     */
    static void addCacheRebuildRunner(String nodeName, String cacheName, Runnable r) {
        cacheRebuildRunner.computeIfAbsent(nodeName, s -> new ConcurrentHashMap<>()).put(cacheName, r);
    }

    /**
     * Registering a consumer for cache rows when creating an index on a node.
     *
     * @param nodeName The name of the node,
     *      the value of which will return {@link GridKernalContext#igniteInstanceName()}.
     * @param idxName Index name.
     * @param c Cache row consumer.
     *
     * @see IndexingTestUtils#nodeName(GridKernalContext)
     * @see IndexingTestUtils#nodeName(IgniteEx)
     * @see IndexingTestUtils#nodeName(GridCacheContext)
     * @see GridCommonAbstractTest#getTestIgniteInstanceName(int)
     */
    static void addIdxCreateCacheRowConsumer(
        String nodeName,
        String idxName,
        IgniteThrowableConsumer<CacheDataRow> c
    ) {
        idxCreateCacheRowConsumer.computeIfAbsent(nodeName, s -> new ConcurrentHashMap<>()).put(idxName, c);
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.sql.calcite;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.IgniteMBeansManager;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcParameterMeta;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryTypeCandidate;
import org.apache.ignite.internal.processors.query.SqlClientContext;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.sql.calcite.iterators.PhysicalOperator;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.jetbrains.annotations.Nullable;

/**
 * TODO: Add class description.
 */
public class CalciteIndexing implements GridQueryIndexing {

    private GridKernalContext ctx;

    private final SchemaPlus rootSchema = Frameworks.createRootSchema(true);



    /** {@inheritDoc} */
    @Override public void start(GridKernalContext ctx, GridSpinBusyLock busyLock) throws IgniteCheckedException {
        System.out.println("CalciteIndexing.start");

        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteCheckedException {
        System.out.println("CalciteIndexing.stop");

        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void registerCache(String cacheName, String schemaName,
        GridCacheContextInfo<?, ?> cacheInfo, Collection<QueryTypeCandidate> cands) throws IgniteCheckedException {
        // Schema ==  schemaName
        // CacheName ignored
        // QueryEntity == table
        SchemaPlus schema = rootSchema.getSubSchema(schemaName);

        if (schema == null) {
            schema = rootSchema.add(schemaName, new AbstractSchema());

            for (QueryTypeCandidate cand : cands) {
                schema.add(cand.descriptor().tableName(), new IgniteTable(cand, cacheName, cand.descriptor().tableName()));
            }
        }
        System.out.println("CalciteIndexing.registerCache");

        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override
    public void unregisterCache(GridCacheContextInfo cacheInfo, boolean rmvIdx) throws IgniteCheckedException {
        System.out.println("CalciteIndexing.unregisterCache");

        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean registerType(GridCacheContextInfo cacheInfo, GridQueryTypeDescriptor desc,
        boolean isSql) throws IgniteCheckedException {
        System.out.println("CalciteIndexing.registerType");

        return false; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> querySqlFields(String schemaName, SqlFieldsQuery qry,
        SqlClientContext cliCtx, boolean keepBinary, boolean failOnMultipleStmts, GridQueryCancel cancel) {

        CalcitePlanner planner = new CalcitePlanner(rootSchema.getSubSchema(schemaName), rootSchema, ctx);

        PhysicalOperator physicalOperator = planner.createPlan(qry.getSql());

        return Collections.singletonList(new QueryCursorImpl<>(physicalOperator));
    }


    /** {@inheritDoc} */
    @Override
    public void store(GridCacheContext cctx, GridQueryTypeDescriptor type, CacheDataRow row, CacheDataRow prevRow,
        boolean prevRowAvailable) throws IgniteCheckedException {
        IgniteTable tbl = (IgniteTable)rootSchema.getSubSchema(type.schemaName()).getTable(type.tableName());

        tbl.incrementRowCount();

    }

    /** {@inheritDoc} */
    @Override public void remove(GridCacheContext cctx, GridQueryTypeDescriptor type,
        CacheDataRow row) throws IgniteCheckedException {
        IgniteTable tbl = (IgniteTable)rootSchema.getSubSchema(type.schemaName()).getTable(type.tableName());

        tbl.decrementRowCount();

    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        System.out.println("CalciteIndexing.onKernalStop");

        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void onClientDisconnect() throws IgniteCheckedException {
        System.out.println("CalciteIndexing.onClientDisconnect");

        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        System.out.println("CalciteIndexing.onDisconnected");

        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public String schema(String cacheName) {
        System.out.println("CalciteIndexing.schema");

        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public Set<String> schemasNames() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public long streamUpdateQuery(String schemaName, String qry, @Nullable Object[] params,
        IgniteDataStreamer<?, ?> streamer) throws IgniteCheckedException {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public List<Long> streamBatchedUpdateQuery(String schemaName, String qry, List<Object[]> params,
        SqlClientContext cliCtx) throws IgniteCheckedException {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(String schemaName, String cacheName,
        String qry, String typeName, IndexingQueryFilter filter) throws IgniteCheckedException {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQuery generateFieldsQuery(String cacheName, SqlQuery qry) {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void dynamicIndexCreate(String schemaName, String tblName, QueryIndexDescriptorImpl idxDesc,
        boolean ifNotExists, SchemaIndexCacheVisitor cacheVisitor) throws IgniteCheckedException {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override
    public void dynamicIndexDrop(String schemaName, String idxName, boolean ifExists) throws IgniteCheckedException {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override
    public void dynamicAddColumn(String schemaName, String tblName, List<QueryField> cols, boolean ifTblExists,
        boolean ifColNotExists) throws IgniteCheckedException {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void dynamicDropColumn(String schemaName, String tblName, List<String> cols, boolean ifTblExists,
        boolean ifColExists) throws IgniteCheckedException {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void destroyOrphanIndex(RootPage page, String indexName, int grpId, PageMemory pageMemory,
        GridAtomicLong removeId, ReuseList reuseList, boolean mvccEnabled) throws IgniteCheckedException {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override
    public UpdateSourceIterator<?> executeUpdateOnDataNodeTransactional(GridCacheContext<?, ?> cctx, int[] ids,
        int[] parts, String schema, String qry, Object[] params, int flags, int pageSize, int timeout,
        AffinityTopologyVersion topVer, MvccSnapshot mvccSnapshot,
        GridQueryCancel cancel) throws IgniteCheckedException {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override
    public List<JdbcParameterMeta> parameterMetaData(String schemaName, SqlFieldsQuery sql) throws IgniteSQLException {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public @Nullable List<GridQueryFieldMetadata> resultMetaData(String schemaName,
        SqlFieldsQuery sql) throws IgniteSQLException {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rebuildIndexesFromHash(GridCacheContext cctx) {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void markAsRebuildNeeded(GridCacheContext cctx) {
        throw new UnsupportedOperationException();// TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public IndexingQueryFilter backupFilter(AffinityTopologyVersion topVer, int[] parts) {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRunningQueryInfo> runningQueries(long duration) {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void cancelQueries(Collection<Long> queries) {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean isStreamableInsertStatement(String schemaName, SqlFieldsQuery sql) throws SQLException {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public GridQueryRowCacheCleaner rowCacheCleaner(int cacheGroupId) {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public @Nullable GridCacheContextInfo registeredCacheInfo(String cacheName) {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean initCacheContext(GridCacheContext ctx) throws IgniteCheckedException {
        throw new UnsupportedOperationException(); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public void registerMxBeans(IgniteMBeansManager mbMgr) throws IgniteCheckedException {
        // TODO: CODE: implement.
    }
}

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

package org.apache.ignite.internal.processors.query;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.IgniteMBeansManager;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.LinkMap;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcParameterMeta;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

/**
 * Abstraction for internal indexing implementation.
 */
public interface GridQueryIndexing {
    /**
     * Starts indexing.
     *
     * @param ctx Context.
     * @param busyLock Busy lock.
     * @throws IgniteCheckedException If failed.
     */
    public void start(GridKernalContext ctx, GridSpinBusyLock busyLock) throws IgniteCheckedException;

    /**
     * Stops indexing.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void stop() throws IgniteCheckedException;

    /**
     * Performs necessary actions on disconnect of a stateful client (say, one associated with a transaction).
     *
     * @throws IgniteCheckedException If failed.
     */
    public void onClientDisconnect() throws IgniteCheckedException;

    /**
     * Generate SqlFieldsQuery from SqlQuery.
     *
     * @param cacheName Cache name.
     * @param qry Query.
     * @return Fields query.
     */
    public SqlFieldsQuery generateFieldsQuery(String cacheName, SqlQuery qry);

    /**
     * Generate SqlFieldsQuery from IndexQuery.
     *
     * @param cacheName Cache name.
     * @param qry Query.
     * @param type Type name.
     * @return Fields query.
     */
    public <K, V> SqlFieldsQuery generateFieldsQuery(String cacheName, IndexQuery<K, V> qry, String type);

    /**
     * Detect whether SQL query should be executed in distributed or local manner and execute it.
     * @param schemaName Schema name.
     * @param qry Query.
     * @param cliCtx Client context.
     * @param keepBinary Keep binary flag.
     * @param failOnMultipleStmts Whether an exception should be thrown for multiple statements query.
     * @return Cursor.
     */
    public List<FieldsQueryCursor<List<?>>> querySqlFields(
        String schemaName,
        SqlFieldsQuery qry,
        SqlClientContext cliCtx,
        boolean keepBinary,
        boolean failOnMultipleStmts,
        GridQueryCancel cancel
    );

    /**
     * Execute an INSERT statement using data streamer as receiver.
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param params Query parameters.
     * @param streamer Data streamer to feed data to.
     * @param qryInitiatorId Query initiator ID.
     * @return Update counter.
     * @throws IgniteCheckedException If failed.
     */
    public long streamUpdateQuery(String schemaName, String qry, @Nullable Object[] params,
        IgniteDataStreamer<?, ?> streamer, String qryInitiatorId) throws IgniteCheckedException;

    /**
     * Execute a batched INSERT statement using data streamer as receiver.
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param params Query parameters.
     * @param cliCtx Client connection context.
     * @param qryInitiatorId Query initiator ID.
     * @return Update counters.
     * @throws IgniteCheckedException If failed.
     */
    public List<Long> streamBatchedUpdateQuery(String schemaName, String qry, List<Object[]> params,
        SqlClientContext cliCtx, String qryInitiatorId) throws IgniteCheckedException;

    /**
     * Executes text query.
     *
     * @param schemaName Schema name.
     * @param cacheName Cache name.
     * @param qry Text query.
     * @param typeName Type name.
     * @param filter Cache name and key filter.
     * @return Queried rows.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(String schemaName, String cacheName,
        String qry, String typeName, IndexingQueryFilter filter) throws IgniteCheckedException;

    /**
     * Executes vector query.
     *
     * @param schemaName Schema name.
     * @param cacheName Cache name.
     * @param field Field name.
     * @param qryVector Vector for the VectorQuery.
     * @param k The number of vectors to return in the VectorQuery.
     * @param typeName Type name.
     * @param filter Cache name and key filter.
     * @param qryVector Vector for the VectorQuery.
     * @param k The number of vectors to return in the VectorQuery.
     * @return Queried rows.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalVector(String schemaName, String cacheName,
        String field, float[] qryVector, int k, float threshold, String typeName, IndexingQueryFilter filter) throws IgniteCheckedException;

    /**
     * Create new index locally.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param idxDesc Index descriptor.
     * @param ifNotExists Ignore operation if index exists (instead of throwing an error).
     * @param cacheVisitor Cache visitor
     * @throws IgniteCheckedException if failed.
     */
    public void dynamicIndexCreate(String schemaName, String tblName, QueryIndexDescriptorImpl idxDesc,
        boolean ifNotExists, SchemaIndexCacheVisitor cacheVisitor) throws IgniteCheckedException;

    /**
     * Remove index from the cache.
     *
     * @param schemaName Schema name.
     * @param idxName Index name.
     * @param ifExists Ignore operation if index does not exist (instead of throwing an error).
     * @throws IgniteCheckedException If failed.
     */
    public void dynamicIndexDrop(String schemaName, String idxName, boolean ifExists) throws IgniteCheckedException;

    /**
     * Add columns to dynamic table.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param cols Columns to add.
     * @param ifTblExists Ignore operation if target table does not exist (instead of throwing an error).
     * @param ifColNotExists Ignore operation if column already exists (instead of throwing an error) - is honored only
     *     for single column case.
     * @throws IgniteCheckedException If failed.
     */
    public void dynamicAddColumn(String schemaName, String tblName, List<QueryField> cols, boolean ifTblExists,
        boolean ifColNotExists) throws IgniteCheckedException;

    /**
     * Drop columns from dynamic table.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param cols Columns to drop.
     * @param ifTblExists Ignore operation if target table does not exist (instead of throwing an error).
     * @param ifColExists Ignore operation if column does not exist (instead of throwing an error) - is honored only
     *     for single column case.
     * @throws IgniteCheckedException If failed.
     */
    public void dynamicDropColumn(String schemaName, String tblName, List<String> cols, boolean ifTblExists,
        boolean ifColExists) throws IgniteCheckedException;

    /**
     * Registers cache.
     *
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param cacheInfo Cache context info.
     * @throws IgniteCheckedException If failed.
     */
    public void registerCache(String cacheName, String schemaName, GridCacheContextInfo<?, ?> cacheInfo)
        throws IgniteCheckedException;

    /**
     * Unregisters cache.
     *
     * @param cacheInfo Cache context info.
     * @param destroy If {@code true}, will remove index.
     * @param clearIdx If {@code true}, will clear the index.
     * @throws IgniteCheckedException If failed to drop cache schema.
     */
    public void unregisterCache(GridCacheContextInfo cacheInfo, boolean destroy, boolean clearIdx) throws IgniteCheckedException;

    /**
     * Destroy founded index which belongs to stopped cache.
     *
     * @param page Root page.
     * @param indexName Index name.
     * @param grpId Group id which contains garbage.
     * @param pageMemory Page memory to work with.
     * @param removeId Global remove id.
     * @param reuseList Reuse list where free pages should be stored.
     * @param mvccEnabled Is mvcc enabled for group or not.
     * @throws IgniteCheckedException If failed.
     */
    public void destroyOrphanIndex(
        RootPage page,
        String indexName,
        int grpId,
        PageMemory pageMemory,
        final GridAtomicLong removeId,
        final ReuseList reuseList,
        boolean mvccEnabled) throws IgniteCheckedException;

    /**
     *
     * @param cctx Cache context.
     * @param ids Involved cache ids.
     * @param parts Partitions.
     * @param schema Schema name.
     * @param qry Query string.
     * @param params Query parameters.
     * @param flags Flags.
     * @param pageSize Fetch page size.
     * @param timeout Timeout.
     * @param topVer Topology version.
     * @param mvccSnapshot MVCC snapshot.
     * @param cancel Query cancel object.
     * @return Cursor over entries which are going to be changed.
     * @throws IgniteCheckedException If failed.
     */
    public UpdateSourceIterator<?> executeUpdateOnDataNodeTransactional(
        GridCacheContext<?, ?> cctx,
        int[] ids,
        int[] parts,
        String schema,
        String qry,
        Object[] params,
        int flags,
        int pageSize,
        int timeout,
        AffinityTopologyVersion topVer,
        MvccSnapshot mvccSnapshot,
        GridQueryCancel cancel
    ) throws IgniteCheckedException;

    /**
     * Registers type if it was not known before or updates it otherwise.
     *
     * @param cacheInfo Cache context info.
     * @param desc Type descriptor.
     * @param isSql {@code true} in case table has been created from SQL.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if type was registered, {@code false} if for some reason it was rejected.
     */
    public boolean registerType(GridCacheContextInfo cacheInfo, GridQueryTypeDescriptor desc,
        boolean isSql) throws IgniteCheckedException;

    /**
     * Jdbc parameters metadata of the specified query.
     *
     * @param schemaName the default schema name for query.
     * @param sql Sql query.
     * @return metadata describing all the parameters, even in case of multi-statement.
     * @throws SQLException if failed to get meta.
     */
    public List<JdbcParameterMeta> parameterMetaData(String schemaName, SqlFieldsQuery sql) throws IgniteSQLException;

    /**
     * Metadata of the result set that is returned if specified query gets executed.
     *
     * @param schemaName the default schema name for query.
     * @param sql Sql query.
     * @return metadata or {@code null} if provided query is multi-statement or id it's not a SELECT statement.
     * @throws SQLException if failed to get meta.
     */
    @Nullable public List<GridQueryFieldMetadata> resultMetaData(String schemaName, SqlFieldsQuery sql)
        throws IgniteSQLException;

    /**
     * Updates index. Note that key is unique for cache, so if cache contains multiple indexes
     * the key should be removed from indexes other than one being updated.
     *
     * @param cctx Cache context.
     * @param type Type descriptor.
     * @param row New row.
     * @param prevRow Previous row.
     * @param prevRowAvailable Whether previous row is available.
     * @throws IgniteCheckedException If failed.
     */
    public void store(GridCacheContext cctx,
        GridQueryTypeDescriptor type,
        CacheDataRow row,
        CacheDataRow prevRow,
        boolean prevRowAvailable) throws IgniteCheckedException;

    /**
     * Removes index entry by key.
     *
     * @param cctx Cache context.
     * @param type Type descriptor.
     * @param row Row.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(GridCacheContext cctx, GridQueryTypeDescriptor type, CacheDataRow row)
        throws IgniteCheckedException;

    /**
     * Rebuild indexes for the given cache if necessary.
     *
     * @param cctx Cache context.
     * @param force Force rebuild indexes.
     * @return Future completed when index rebuild finished.
     */
    @Nullable IgniteInternalFuture<?> rebuildIndexesFromHash(GridCacheContext cctx, boolean force);

    /**
     * Rebuild in-memory indexes for the given cache if necessary.
     *
     * @param cctx Cache context.
     * @return Future completed when indexes rebuild finished.
     */
    @Nullable IgniteInternalFuture<?> rebuildInMemoryIndexes(GridCacheContext cctx);

    /**
     * Mark as rebuild needed for the given cache.
     *
     * @param cctx Cache context.
     */
    void markAsRebuildNeeded(GridCacheContext cctx);

    /**
     * Returns backup filter.
     *
     * @param topVer Topology version.
     * @param parts Partitions.
     * @return Backup filter.
     */
    public IndexingQueryFilter backupFilter(AffinityTopologyVersion topVer, int[] parts);

    /**
     * Client disconnected callback.
     *
     * @param reconnectFut Reconnect future.
     */
    public void onDisconnected(IgniteFuture<?> reconnectFut);

    /**
     * Collect queries that already running more than specified duration.
     *
     * @param duration Duration to check.
     * @return Collection of long running queries.
     */
    public Collection<GridRunningQueryInfo> runningQueries(long duration);

    /**
     * Cancel specified queries.
     *
     * @param queries Queries ID's to cancel.
     */
    public void cancelQueries(Collection<Long> queries);

    /**
     * Callback executed after the kernal started.
     */
    public void onKernalStart();

    /**
     * Cancels all executing queries.
     */
    public void onKernalStop();

    /**
     * Gets database schema from cache name.
     *
     * @param cacheName Cache name. {@code null} would be converted to an empty string.
     * @return Schema name. Should not be null since we should not fail for an invalid cache name.
     */
    public String schema(String cacheName);

    /**
     * Gets database schemas names.
     *
     * @return Schema names.
     */
    public Set<String> schemasNames();

    /**
     * Whether passed sql statement is single insert statement eligible for streaming.
     *
     * @param schemaName name of the schema.
     * @param sql sql statement.
     */
    public boolean isStreamableInsertStatement(String schemaName, SqlFieldsQuery sql) throws SQLException;

    /**
     * Return row cache cleaner.
     *
     * @param cacheGroupId Cache group id.
     * @return Row cache cleaner.
     */
    public GridQueryRowCacheCleaner rowCacheCleaner(int cacheGroupId);

    /**
     * Return context for registered cache info.
     *
     * @param cacheName Cache name.
     * @return Cache context for registered cache or {@code null} in case the cache has not been registered.
     */
    @Nullable public GridCacheContextInfo registeredCacheInfo(String cacheName);

    /**
     * Clear cache info and clear parser cache on call cache.close() on client node.
     *
     * @param cacheName Cache name to clear.
     */
    public void closeCacheOnClient(String cacheName);

    /**
     * Initialize table's cache context created for not started cache.
     *
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     *
     * @return {@code true} If context has been initialized.
     */
    public boolean initCacheContext(GridCacheContext ctx) throws IgniteCheckedException;

    /**
     * Register SQL JMX beans.
     *
     * @param mbMgr Ignite MXBean manager.
     * @throws IgniteCheckedException On bean registration error.
     */
    void registerMxBeans(IgniteMBeansManager mbMgr) throws IgniteCheckedException;

    /**
     * Return table information filtered by given patterns.
     *
     * @param schemaNamePtrn Filter by schema name. Can be {@code null} to don't use the filter.
     * @param tblNamePtrn Filter by table name. Can be {@code null} to don't use the filter.
     * @param tblTypes Filter by table type. As Of now supported only 'TABLES' and 'VIEWS'.
     * Can be {@code null} or empty to don't use the filter.
     *
     * @return Column information filtered by given patterns.
     */
    Collection<TableInformation> tablesInformation(String schemaNamePtrn, String tblNamePtrn, String... tblTypes);

    /**
     * Return column information filtered by given patterns.
     *
     * @param schemaNamePtrn Filter by schema name. Can be {@code null} to don't use the filter.
     * @param tblNamePtrn Filter by table name. Can be {@code null} to don't use the filter.
     * @param colNamePtrn Filter by column name. Can be {@code null} to don't use the filter.
     *
     * @return Column information filtered by given patterns.
     */
    Collection<ColumnInformation> columnsInformation(String schemaNamePtrn, String tblNamePtrn, String colNamePtrn);

    /**
     * Return index size by schema, table and index name.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @return Index size (Number of elements) or {@code 0} if index not found.
     */
    default long indexSize(String schemaName, String tblName, String idxName) throws IgniteCheckedException {
        return 0;
    }

    /**
     * Information about secondary indexes efficient (actual) inline size.
     *
     * @return Map with inline sizes. The key of entry is a full index name (with schema and table name), the value of
     * entry is a inline size.
     */
    default Map<String, Integer> secondaryIndexesInlineSize() {
        return Collections.emptyMap();
    }

    /**
     * Setup cluster timezone ID used for date time conversion.
     *
     * @param tz Cluster timezone.
     */
    default void clusterTimezone(TimeZone tz) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * Gets cluster SQL timezone used for date time conversion.
     *
     * @return Cluster SQL timezone.
     */
    default TimeZone clusterTimezone() {
        return TimeZone.getDefault();
    }

    /**
     * Defragment index partition.
     *
     * @param grpCtx Old group context.
     * @param newCtx New group context.
     * @param partPageMem Partition page memory.
     * @param mappingByPartition Mapping page memory.
     * @param cpLock Defragmentation checkpoint read lock.
     * @param cancellationChecker Cancellation checker.
     * @param defragmentationThreadPool Thread pool for defragmentation.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void defragment(
        CacheGroupContext grpCtx,
        CacheGroupContext newCtx,
        PageMemoryEx partPageMem,
        IntMap<LinkMap> mappingByPartition,
        CheckpointTimeoutLock cpLock,
        Runnable cancellationChecker,
        IgniteThreadPoolExecutor defragmentationThreadPool
    ) throws IgniteCheckedException;

    /**
     * Marks index as renamed.
     *
     * @param cacheCtx Cache context.
     * @param indexTreeName Index tree name.
     */
    void markIndexRenamed(GridCacheContext<?, ?> cacheCtx, String indexTreeName);

    /**
     * Checks if object of the specified class can be stored in the specified table column by the query engine.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param colName Name of the column.
     * @param cls Class to perform check on.
     * @return Whether object of the specified class can be successfully stored and accessed from the SQL column by the
     *         query engine.
     * @throws IgniteSQLException if table or column with specified name was not found.
     */
    boolean isConvertibleToColumnType(String schemaName, String tblName, String colName, Class<?> cls);
}

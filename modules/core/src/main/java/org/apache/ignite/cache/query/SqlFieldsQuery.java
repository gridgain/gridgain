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

package org.apache.ignite.cache.query;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * SQL Fields query. This query can return specific fields of data based
 * on SQL {@code 'select'} clause.
 * <h1 class="header">Collocated Flag</h1>
 * Collocation flag is used for optimization purposes. Whenever Ignite executes
 * a distributed query, it sends sub-queries to individual cluster members.
 * If you know in advance that the elements of your query selection are collocated
 * together on the same node, usually based on some <b>affinity-key</b>, Ignite
 * can make significant performance and network optimizations.
 * <p>
 * For example, in case of Word-Count example, we know that all identical words
 * are processed on the same cluster member, because we use the {@code word} itself
 * as affinity key. This allows Ignite to execute the {@code 'limit'} clause on
 * the remote nodes and bring back only the small data set specified within the 'limit' clause,
 * instead of the whole query result as would happen in a non-collocated execution.
 *
 * @see IgniteCache#query(Query)
 */
public class SqlFieldsQuery extends Query<List<?>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default value of the update internal batch size. */
    private static final int DFLT_UPDATE_BATCH_SIZE = 1;

    /** Threaded query originator. */
    private static ThreadLocal<String> threadedQryInitiatorId = new ThreadLocal<>();

    /** Default value of Query timeout. Default is -1 means no timeout is set. */
    private static final int DFLT_QUERY_TIMEOUT = -1;

    /** Lazy mode. */
    @SuppressWarnings("NonConstantFieldWithUpperCaseName")
    public static boolean DFLT_LAZY = true;

    /** SQL Query. */
    private String sql;

    /** Arguments. */
    @GridToStringInclude
    private Object[] args;

    /** Collocation flag. */
    private boolean collocated;

    /** Query timeout in millis. */
    private int timeout = DFLT_QUERY_TIMEOUT;

    /** */
    private boolean enforceJoinOrder;

    /** */
    private boolean distributedJoins;

    /** */
    private boolean replicatedOnly;

    /**
     * Lazy mode is default since GG 8.9.1.
     */
    private boolean lazy = DFLT_LAZY;

    /** Partitions for query */
    private int[] parts;

    /** Schema. */
    private String schema;

    /**
     * Update internal batch size. Default is 1 to prevent deadlock on update where keys sequence are different in
     * several concurrent updates.
     */
    private int updateBatchSize = DFLT_UPDATE_BATCH_SIZE;

    /**
     * Query's originator string (client host+port, user name,
     * job name or any user's information about query initiator).
     */
    private String qryInitiatorId;

    /** Whether server side DML should be enabled. */
    private boolean skipReducerOnUpdate;

    /** Query label. */
    private @Nullable String label;

    /**
     * Copy constructs SQL fields query.
     *
     * @param qry SQL query.
     */
    public SqlFieldsQuery(SqlFieldsQuery qry) {
        sql = qry.sql;
        args = qry.args;
        collocated = qry.collocated;
        timeout = qry.timeout;
        enforceJoinOrder = qry.enforceJoinOrder;
        distributedJoins = qry.distributedJoins;
        replicatedOnly = qry.replicatedOnly;
        lazy = qry.lazy;
        parts = qry.parts;
        schema = qry.schema;
        updateBatchSize = qry.updateBatchSize;
        qryInitiatorId = qry.qryInitiatorId;
        skipReducerOnUpdate = qry.skipReducerOnUpdate;
        label = qry.label;
    }

    /**
     * Constructs SQL fields query.
     *
     * @param sql SQL query.
     */
    public SqlFieldsQuery(String sql) {
        setSql(sql);
    }

    /**
     * Constructs SQL fields query.
     *
     * @param sql SQL query.
     * @param collocated Collocated flag.
     */
    public SqlFieldsQuery(String sql, boolean collocated) {
        this.sql = sql;
        this.collocated = collocated;
    }

    /**
     * Gets SQL clause.
     *
     * @return SQL clause.
     */
    public String getSql() {
        return sql;
    }

    /**
     * Sets SQL clause.
     *
     * @param sql SQL clause.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setSql(String sql) {
        A.notNull(sql, "sql");

        this.sql = sql;

        return this;
    }

    /**
     * Gets SQL arguments.
     *
     * @return SQL arguments.
     */
    public Object[] getArgs() {
        return args;
    }

    /**
     * Sets SQL arguments.
     *
     * @param args SQL arguments.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setArgs(Object... args) {
        this.args = args;

        return this;
    }

    /**
     * Gets the query execution timeout in milliseconds.
     *
     * @return Timeout value.
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Sets the query execution timeout. Query will be automatically cancelled if the execution timeout is exceeded.
     * @param timeout Timeout value. Zero value disables timeout.
     * @param timeUnit Time unit.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setTimeout(int timeout, TimeUnit timeUnit) {
        this.timeout = QueryUtils.validateTimeout(timeout, timeUnit);

        return this;
    }

    /**
     * Switches update mode on server side.
     * <p>
     * By default, when processing DML command, Ignite first fetches all affected intermediate rows for analysis to the
     * node which initiated the query and only then forms batches of updated values to be sent to remote nodes.
     * For simple DML commands (that however affect great deal of rows) such approach may be an overkill in terms of
     * network delays and memory usage on initiating node. Use this flag as hint for Ignite to do all intermediate rows
     * analysis and updates in place on corresponding remote data nodes.
     * <p>
     * <b>IMPORTANT NOTE</b>: There are limitations to what DML command can be optimized this way. The command containing LIMIT, OFFSET,
     * DISTINCT, ORDER BY, GROUP BY, sub-query or UNION will be processed the usual way despite this flag setting.
     * <p>
     * Defaults to {@code false}, meaning that intermediate results will be fetched to initiating node first.
     * Only affects DML commands. Ignored when {@link #isLocal()} is {@code true}.
     * Note that when set to {@code true}, the query may fail in the case of even single node failure.
     *
     * @param skipReducerOnUpdate Server side update flag.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setSkipReducerOnUpdate(boolean skipReducerOnUpdate) {
        this.skipReducerOnUpdate = skipReducerOnUpdate;

        return this;
    }

    /**
     * Gets server side update flag.
     * <p>
     * See {@link #setSkipReducerOnUpdate(boolean)} for more information.
     *
     * @return Server side update flag.
     */
    public boolean isSkipReducerOnUpdate() {
        return skipReducerOnUpdate;
    }

    /**
     * Checks if this query is collocated.
     *
     * @return {@code true} If the query is collocated.
     */
    public boolean isCollocated() {
        return collocated;
    }

    /**
     * Sets flag defining if this query is collocated.
     *
     * Collocation flag is used for optimization purposes of queries with GROUP BY statements.
     * Whenever Ignite executes a distributed query, it sends sub-queries to individual cluster members.
     * If you know in advance that the elements of your query selection are collocated together on the same node and
     * you group by collocated key (primary or affinity key), then Ignite can make significant performance and network
     * optimizations by grouping data on remote nodes.
     *
     * @param collocated Flag value.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setCollocated(boolean collocated) {
        this.collocated = collocated;

        return this;
    }

    /**
     * Checks if join order of tables if enforced.
     *
     * @return Flag value.
     */
    public boolean isEnforceJoinOrder() {
        return enforceJoinOrder;
    }

    /**
     * Sets flag to enforce join order of tables in the query. If set to {@code true}
     * query optimizer will not reorder tables in join. By default is {@code false}.
     * <p>
     * It is not recommended to enable this property until you are sure that
     * your indexes and the query itself are correct and tuned as much as possible but
     * query optimizer still produces wrong join order.
     *
     * @param enforceJoinOrder Flag value.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setEnforceJoinOrder(boolean enforceJoinOrder) {
        this.enforceJoinOrder = enforceJoinOrder;

        return this;
    }

    /**
     * Specify if distributed joins are enabled for this query.
     *
     * @param distributedJoins Distributed joins enabled.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setDistributedJoins(boolean distributedJoins) {
        this.distributedJoins = distributedJoins;

        return this;
    }

    /**
     * Check if distributed joins are enabled for this query.
     *
     * @return {@code true} If distributed joins enabled.
     */
    public boolean isDistributedJoins() {
        return distributedJoins;
    }

    /**
     * Gets query label.
     *
     * @return Query label, or {@code null} if not set.
     */
    public @Nullable String getLabel() {
        return label;
    }

    /**
     * Sets query label.
     * <p>
     * The specified label can be used to identify the running query in system views
     * and in the log when printing warnings about long-running queries.
     *
     * @param label Query label, or {@code null} to unset.
     * @return {@code this} for chaining.
     */
    public SqlFieldsQuery setLabel(@Nullable String label) {
        this.label = label;

        return this;
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQuery setPageSize(int pageSize) {
        return (SqlFieldsQuery)super.setPageSize(pageSize);
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQuery setLocal(boolean loc) {
        return (SqlFieldsQuery)super.setLocal(loc);
    }

    /**
     * Specify if the query contains only replicated tables.
     * This is a hint for potentially more effective execution.
     *
     * @param replicatedOnly The query contains only replicated tables.
     * @return {@code this} For chaining.
     * @deprecated No longer used as of Apache Ignite 2.8.
     */
    @Deprecated
    public SqlFieldsQuery setReplicatedOnly(boolean replicatedOnly) {
        this.replicatedOnly = replicatedOnly;

        return this;
    }

    /**
     * Check is the query contains only replicated tables.
     *
     * @return {@code true} If the query contains only replicated tables.
     * @deprecated No longer used as of Apache Ignite 2.8.
     */
    @Deprecated
    public boolean isReplicatedOnly() {
        return replicatedOnly;
    }

    /**
     * Sets lazy query execution flag.
     * <p>
     * If {@code lazy=false} Ignite will attempt to fetch the whole query result set to memory and send it to the client. For small
     * and medium result sets this provides optimal performance and minimize duration of internal database locks, thus
     * increasing concurrency.
     * <p>
     * If result set is too big to fit in available memory this could lead to excessive GC pauses and even
     * {@link OutOfMemoryError}. Use this flag as a hint for Ignite to fetch result set lazily, thus minimizing memory
     * consumption at the cost of moderate performance hit.
     * <p>
     * Defaults to {@code true}, meaning that the only first page of result set is fetched to memory.
     *
     * @param lazy Lazy query execution flag.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setLazy(boolean lazy) {
        this.lazy = lazy;

        return this;
    }

    /**
     * Gets lazy query execution flag.
     * <p>
     * See {@link #setLazy(boolean)} for more information.
     *
     * @return Lazy flag.
     */
    public boolean isLazy() {
        return lazy;
    }

    /**
     * Gets partitions for query, in ascending order.
     */
    public int[] getPartitions() {
        return parts;
    }

    /**
     * Sets partitions for a query.
     * The query will be executed only on nodes which are primary for specified partitions.
     * <p>
     * Note what passed array'll be sorted in place for performance reasons, if it wasn't sorted yet.
     *
     * @param parts Partitions.
     * @return {@code this} for chaining.
     */
    public SqlFieldsQuery setPartitions(int... parts) {
        this.parts = prepare(parts);

        return this;
    }

    /**
     * Get schema for the query.
     * If not set, current cache name is used, which means you can
     * omit schema name for tables within the current cache.
     *
     * @return Schema. {@code null} if schema is not set.
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Set schema for the query.
     * If not set, current cache name is used, which means you can
     * omit schema name for tables within the current cache.
     *
     * @param schema Schema. {@code null} to unset schema.
     * @return {@code this} for chaining.
     */
    public SqlFieldsQuery setSchema(String schema) {
        this.schema = schema;

        return this;
    }

    /**
     * Gets update internal bach size.
     * Default is 1 to prevent deadlock on update where keys sequence are different in
     * several concurrent updates.
     *
     * @return Update internal batch size
     */
    public int getUpdateBatchSize() {
        return updateBatchSize;
    }

    /**
     * Sets update internal bach size.
     * Default is 1 to prevent deadlock on update where keys sequence are different in
     * several concurrent updates.
     *
     * @param updateBatchSize Update internal batch size.
     * @return {@code this} for chaining.
     */
    public SqlFieldsQuery setUpdateBatchSize(int updateBatchSize) {
        A.ensure(updateBatchSize >= 1, "updateBatchSize cannot be lower than 1");

        this.updateBatchSize = updateBatchSize;

        return this;
    }

    /**
     * @return Query's initiator identifier string (client host+port, user name,
     *       job name or any user's information about query initiator).
     */
    public String getQueryInitiatorId() {
        return qryInitiatorId;
    }

    /**
     * @param qryInitiatorId Query's initiator identifier string (client host+port, user name,
     *      job name or any user's information about query initiator).
     *
     * @return {@code this} for chaining.
     */
    public SqlFieldsQuery setQueryInitiatorId(String qryInitiatorId) {
        this.qryInitiatorId = qryInitiatorId;

        return this;
    }

    /**
     * @return Copy of this query.
     */
    public SqlFieldsQuery copy() {
        return new SqlFieldsQuery(this);
    }

    /**
     * Used at the Job worker to setup originator by default for current thread.
     *
     * @param originator Query's originator string.
     */
    public static void setThreadedQueryInitiatorId(String originator) {
        threadedQryInitiatorId.set(originator);
    }

    /**
     * Used at the job worker to clear originator for current thread.
     */
    public static void resetThreadedQueryInitiatorId() {
        threadedQryInitiatorId.remove();
    }

    /**
     * @return originator set up by the job worker.
     */
    public static String threadedQueryInitiatorId() {
        return threadedQryInitiatorId.get();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlFieldsQuery.class, this);
    }
}

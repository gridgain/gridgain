/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.configuration;

import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * The configuration of the SQL query subsystem.
 */
public class SqlConfiguration {
    /** Default SQL query history size. */
    public static final int DFLT_SQL_QUERY_HISTORY_SIZE = 1000;

    /** Default timeout after which long query warning will be printed. */
    public static final long DFLT_LONG_QRY_WARN_TIMEOUT = 1000;

    /** Default SQL query global memory quota. */
    public static final String DFLT_SQL_QUERY_GLOBAL_MEMORY_QUOTA = "60%"; // 60% of heap.

    /** Default SQL per query memory quota. */
    public static final String DFLT_SQL_QUERY_MEMORY_QUOTA = "0";

    /** Default value for SQL offloading flag. */
    public static final boolean DFLT_SQL_QUERY_OFFLOADING_ENABLED = false;

    /** Default query timeout. */
    public static final long DFLT_QRY_TIMEOUT = 0;

    /** */
    private long longQryWarnTimeout = DFLT_LONG_QRY_WARN_TIMEOUT;

    /** SQL schemas to be created on node start. */
    private String[] sqlSchemas;

    /** SQL query history size. */
    private int sqlQryHistSize = DFLT_SQL_QUERY_HISTORY_SIZE;

    /** Global memory quota. */
    private String sqlGlobalMemoryQuota = DFLT_SQL_QUERY_GLOBAL_MEMORY_QUOTA;

    /** Per query memory quota. */
    private String sqlQueryMemoryQuota = DFLT_SQL_QUERY_MEMORY_QUOTA;

    /** Offloading enabled flag - whether to start offloading where quota is exceeded or throw an exception. */
    private boolean sqlOffloadingEnabled = DFLT_SQL_QUERY_OFFLOADING_ENABLED;

    /** Default query timeout. */
    private long dfltQryTimeout = DFLT_QRY_TIMEOUT;

    /**
     * Number of SQL query history elements to keep in memory. If not provided, then default value {@link
     * #DFLT_SQL_QUERY_HISTORY_SIZE} is used. If provided value is less or equals 0, then gathering SQL query history
     * will be switched off.
     *
     * @return SQL query history size.
     */
    public int getSqlQueryHistorySize() {
        return sqlQryHistSize;
    }

    /**
     * Sets number of SQL query history elements kept in memory. If not explicitly set, then default value is {@link
     * #DFLT_SQL_QUERY_HISTORY_SIZE}.
     *
     * @param size Number of SQL query history elements kept in memory.
     * @return {@code this} for chaining.
     */
    public SqlConfiguration setSqlQueryHistorySize(int size) {
        sqlQryHistSize = size;

        return this;
    }

    /**
     * Gets SQL schemas to be created on node startup.
     * <p>
     * See {@link #setSqlSchemas(String...)} for more information.
     *
     * @return SQL schemas to be created on node startup.
     */
    public String[] getSqlSchemas() {
        return sqlSchemas;
    }

    /**
     * Sets SQL schemas to be created on node startup. Schemas are created on local node only and are not propagated
     * to other cluster nodes. Created schemas cannot be dropped.
     * <p>
     * By default schema names are case-insensitive, i.e. {@code my_schema} and {@code My_Schema} represents the same
     * object. Use quotes to enforce case sensitivity (e.g. {@code "My_Schema"}).
     * <p>
     * Property is ignored if {@code ignite-indexing} module is not in classpath.
     *
     * @param sqlSchemas SQL schemas to be created on node startup.
     * @return {@code this} for chaining.
     */
    public SqlConfiguration setSqlSchemas(String... sqlSchemas) {
        this.sqlSchemas = sqlSchemas;

        return this;
    }

    /**
     * Gets timeout in milliseconds after which long query warning will be printed.
     *
     * @return Timeout in milliseconds.
     */
    public long getLongQueryWarningTimeout() {
        return longQryWarnTimeout;
    }

    /**
     * Sets timeout in milliseconds after which long query warning will be printed.
     *
     * @param longQryWarnTimeout Timeout in milliseconds.
     * @return {@code this} for chaining.
     */
    public SqlConfiguration setLongQueryWarningTimeout(long longQryWarnTimeout) {
        this.longQryWarnTimeout = longQryWarnTimeout;

        return this;
    }

    /**
     * Returns global memory pool size for SQL queries.
     * <p>
     * See {@link #setSqlGlobalMemoryQuota(String)} for details.
     *
     * @return Global memory pool size for SQL queries.
     */
    public String getSqlGlobalMemoryQuota() {
        return sqlGlobalMemoryQuota;
    }

    /**
     * Sets global memory pool size for SQL queries.
     * <p>
     * Global SQL query memory pool size or SQL query memory quota - is an upper bound for the heap memory part
     * which might be occupied by the SQL query execution engine. This quota is shared among all simultaneously
     * running queries, hence it be easily consumed by the single heavy analytics query. If you want to control
     * memory quota on per-query basis consider {@link #setSqlQueryMemoryQuota(String)}.
     * <p>
     * There are two options of query behaviour when either query or global memory quota is exceeded:
     * <ul>
     *     <li> If disk offloading is disabled, the query caller gets an error that quota was exceeded. </li>
     *     <li> If disk offloading is enabled, the intermediate query results will be offloaded to a disk. </li>
     * </ul>
     * See {@link #setSqlOffloadingEnabled(boolean)} for details.
     * <p>
     * If not provided, the default value is defined by {@link #DFLT_SQL_QUERY_GLOBAL_MEMORY_QUOTA}.
     * <p>
     * The value is specified as string value of size of in bytes.
     * <p>
     * Value {@code 0} means no quota at all.
     *  <p>
     *  Quota may be specified also in:
     *  <ul>
     *      <li>Kilobytes - just append the letter 'K' or 'k': {@code 10k, 400K}</li>
     *      <li>Megabytes - just append the letter 'M' or 'm': {@code 60m, 420M}</li>
     *      <li>Gigabytes - just append the letter 'G' or 'g': {@code 7g, 2G}</li>
     *      <li>Percent of heap - just append the sign '%': {@code 45%, 80%}</li>
     *  </ul>
     *
     * @param size Size of global memory pool for SQL queries in bytes, kilobytes, megabytes,
     * or percentage of the max heap.
     * @return {@code this} for chaining.
     */
    public SqlConfiguration setSqlGlobalMemoryQuota(String size) {
        this.sqlGlobalMemoryQuota = size;

        return this;
    }

    /**
     * Returns per-query memory quota.
     * See {@link #setSqlQueryMemoryQuota(String)} for details.
     *
     * @return Per-query memory quota.
     */
    public String getSqlQueryMemoryQuota() {
        return sqlQueryMemoryQuota;
    }

    /**
     * Sets per-query memory quota.
     * <p>
     * It is the maximum amount of memory intended for the particular single query execution.
     * If a query execution exceeds this bound, the either would happen:
     * <ul>
     *     <li> If disk offloading is disabled, the query caller gets an error that quota was exceeded. </li>
     *     <li> If disk offloading is enabled, the intermediate query results will be offloaded to a disk. </li>
     * </ul>
     * See {@link #setSqlOffloadingEnabled(boolean)} for details.
     * <p>
     * If not provided, the default value is defined by {@link #DFLT_SQL_QUERY_MEMORY_QUOTA}.
     * <p>
     * The value is specified as string value of size of in bytes.
     * <p>
     * Value {@code 0} means no quota at all.
     *  <p>
     *  Quota may be specified also in:
     *  <ul>
     *      <li>Kilobytes - just append the letter 'K' or 'k': {@code 10k, 400K}</li>
     *      <li>Megabytes - just append the letter 'M' or 'm': {@code 60m, 420M}</li>
     *      <li>Gigabytes - just append the letter 'G' or 'g': {@code 7g, 2G}</li>
     *      <li>Percent of the heap - just append the sign '%': {@code 45%, 80%}</li>
     *  </ul>
     *
     * @param size Size of per-query memory quota in bytes, kilobytes, megabytes, or percentage of the max heap.
     * @return {@code this} for chaining.
     */
    public SqlConfiguration setSqlQueryMemoryQuota(String size) {
        this.sqlQueryMemoryQuota = size;

        return this;
    }

    /**
     * Returns flag whether disk offloading is enabled.
     * See {@link #setSqlOffloadingEnabled(boolean)} for details.
     *
     * @return Flag whether disk offloading is enabled.
     */
    public boolean isSqlOffloadingEnabled() {
        return sqlOffloadingEnabled;
    }

    /**
     * Sets the SQL query offloading enabled flag.
     * <p>
     * Offloading flag specifies the query execution behavior on memory quota excess - either global quota
     * (see {@link #setSqlGlobalMemoryQuota(String)}) or per query quota (see {@link #setSqlQueryMemoryQuota(String)}).
     * Possible options on quota excess are:
     * <ul>
     *     <li>
     *         If {@code offloadingEnabled} flag set to {@code false}, the exception will be thrown when
     *         memory quota is exceeded.
     *     </li>
     *     <li>
     *         If {@code offloadingEnabled} flag set to {@code true}, the intermediate query results will be
     *         offloaded to disk. It may slow down the query execution time by orders of magnitude, but eventually
     *         the query will be executed and the caller will get a result.
     *     </li>
     * </ul>
     *
     * If not provided, the default value is {@code false}.
     *
     * @param offloadingEnabled Offloading enabled flag.
     * @return {@code this} for chaining.
     */
    public SqlConfiguration setSqlOffloadingEnabled(boolean offloadingEnabled) {
        this.sqlOffloadingEnabled = offloadingEnabled;

        return this;
    }

    /**
     * Defines the default query timeout.
     *
     * Defaults to {@link #DFLT_QRY_TIMEOUT}.
     * {@code 0} means there is no timeout (this
     * is a default value)
     *
     * @return Default query timeout.
     * @deprecated Since 8.7.26. Please use distributed default query timeout.
     */
    @Deprecated
    public long getDefaultQueryTimeout() {
        return dfltQryTimeout;
    }

    /**
     * Sets timeout in milliseconds for default query timeout.
     * {@code 0} means there is no timeout (this
     * is a default value)
     *
     * @param dfltQryTimeout Timeout in milliseconds.
     * @return {@code this} for chaining.
     * @deprecated Since 8.7.26. Please use distributed default query timeout.
     */
    @Deprecated
    public SqlConfiguration setDefaultQueryTimeout(long dfltQryTimeout) {
        A.ensure(dfltQryTimeout >= 0 && dfltQryTimeout <= Integer.MAX_VALUE,
            "default query timeout value should be not negative Integer.");

        this.dfltQryTimeout = dfltQryTimeout;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlConfiguration.class, this);
    }
}

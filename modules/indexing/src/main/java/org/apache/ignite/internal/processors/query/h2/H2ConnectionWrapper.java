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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.jdbc.JdbcStatement;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper to store connection with currently used schema and statement cache.
 */
public class H2ConnectionWrapper implements AutoCloseable {
    /** */
    private static final int STATEMENT_CACHE_SIZE = 256;

    /** */
    private final Connection conn;

    /** Connection manager. */
    private final ConnectionManager connMgr;

    /** */
    private volatile String schema;

    /** */
    private volatile H2StatementCache statementCache;

    /** Used flag. */
    public AtomicBoolean used = new AtomicBoolean(true);

    /**
     * @param conn Connection to use.
     * @param connMgr Connection manager is use to recycle connection
     *      (connection is closed or returned to connection pool).
     */
    H2ConnectionWrapper(Connection conn, ConnectionManager connMgr) {
        this.conn = conn;
        this.connMgr = connMgr;

        initStatementCache();
    }

    /**
     * @return Schema name if schema is set, null otherwise.
     */
    public String schema() {
        return schema;
    }

    /**
     * @param schema Schema name set on this connection.
     */
    public void schema(@Nullable String schema) {
        if (schema != null && !F.eq(this.schema, schema)) {
            try {
                this.schema = schema;

                conn.setSchema(schema);
            }
            catch (SQLException e) {
                throw new IgniteSQLException("Failed to set schema for DB connection for thread [schema=" +
                    schema + "]", e);
            }
        }
    }

    /**
     * @return Connection.
     */
    public Connection connection() {
        return conn;
    }

    /**
     * @return Statement cache corresponding to connection.
     */
    public H2StatementCache statementCache() {
        return statementCache;
    }

    /**
     * Clears statement cache.
     */
    public void clearStatementCache() {
        initStatementCache();
    }

    /**
     * @return Statement cache size.
     */
    public int statementCacheSize() {
        return statementCache == null ? 0 : statementCache.size();
    }

    /**
     * Initializes statement cache.
     */
    private void initStatementCache() {
        statementCache = new H2StatementCache(STATEMENT_CACHE_SIZE);
    }


    /**
     * Prepare statement caching it if needed.
     *
     * @param sql SQL.
     * @return Prepared statement.
     * @throws SQLException If failed.
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        PreparedStatement stmt = cachedPreparedStatement(sql);

        if (stmt == null) {
            H2CachedStatementKey key = new H2CachedStatementKey(schema, sql);

            stmt = prepareStatementNoCache(sql);

            statementCache.put(key, stmt);
        }

        return stmt;
    }

    /**
     * Get cached prepared statement (if any).
     *
     * @param sql SQL.
     * @return Prepared statement or {@code null}.
     * @throws SQLException On error.
     */
    @Nullable public PreparedStatement cachedPreparedStatement(String sql) throws SQLException {
        H2CachedStatementKey key = new H2CachedStatementKey(schema, sql);

        PreparedStatement stmt = statementCache.get(key);

        // Nothing found.
        if (stmt == null)
            return null;

        // Is statement still valid?
        if (
            stmt.isClosed() ||                                 // Closed.
                stmt.unwrap(JdbcStatement.class).isCancelled() ||  // Cancelled.
                GridSqlQueryParser.prepared(stmt).needRecompile() // Outdated (schema has been changed concurrently).
        ) {
            statementCache.remove(schema, sql);

            return null;
        }

        return stmt;
    }

    /**
     * Get prepared statement without caching.
     *
     * @param sql SQL.
     * @return Prepared statement.
     * @throws SQLException If failed.
     */
    public PreparedStatement prepareStatementNoCache(String sql) throws SQLException {
        boolean insertHack = GridH2Table.insertHackRequired(sql);

        if (insertHack) {
            GridH2Table.insertHack(true);

            try {
                return conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            }
            finally {
                GridH2Table.insertHack(false);
            }
        }
        else
            return conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    /**
     *
     */
    void use() {
        if (!used.compareAndSet(false, true))
            assert false : "Invalid connection state";
    }

    public volatile Exception oncreate;
    public volatile Exception onclose;

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2ConnectionWrapper.class, this);
    }

    /** Closes wrapped connection (return to pool or close). */
    @Override public void close() {
        H2Utils.resetSession(this);

        if (!used.compareAndSet(true, false)) {
            oncreate.printStackTrace();
            onclose.printStackTrace();
            assert false : "Invalid connection state";
        }

        onclose = new Exception("onclose");

        connMgr.recycle(this);
    }
}

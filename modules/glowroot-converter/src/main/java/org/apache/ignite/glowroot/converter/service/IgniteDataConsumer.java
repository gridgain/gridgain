/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.glowroot.converter.service;

import org.apache.ignite.glowroot.converter.model.CacheConfigMeta;
import org.apache.ignite.glowroot.converter.model.CacheQueryTraceItem;
import org.apache.ignite.glowroot.converter.model.CacheTraceItem;
import org.apache.ignite.glowroot.converter.model.CommitTraceItem;
import org.apache.ignite.glowroot.converter.model.ComputeTraceItem;
import org.apache.ignite.glowroot.converter.model.TraceItem;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class that persists glowroot traces to Ignite.
 */
public class IgniteDataConsumer implements AutoCloseable {
    /** **/
    private static final Logger logger = Logger.getLogger(IgniteDataConsumer.class.getName());

    /** jdbc thin connection to Ignite. **/
    private final Connection conn;

    /** Prepared statement to populate cache ops table. **/
    private final PreparedStatement populateCachePreparedStmt;

    /** Prepared statement to populate cache query table. **/
    private final PreparedStatement populateCacheQryPreparedStmt;

    /** Prepared statement to populate compute table. **/
    private final PreparedStatement populateComputePreparedStmt;

    /** Prepared statement to populate transactions commit table. **/
    private final PreparedStatement populateTxCommitPreparedStmt;

    /** Prepared statement to populate cache configurations. **/
    private final PreparedStatement populateCacheCfgPreparedStmt;

    /**
     * Establish connection to Ignite cluster, prepares some jdbc statements and set streaming mode.
     *
     * @param igniteJdbcConnStr Ignite jdbc thin connection string.
     * @param cleanupAllData Boolean param that forses all data and schema cleanup as initial step.
     * @param overwriteEntries Boolean param that forses overwrite mode.
     * @throws SQLException If failed during jdbc operations processing.
     */
    public IgniteDataConsumer(String igniteJdbcConnStr, boolean cleanupAllData,
        boolean overwriteEntries) throws SQLException {
        conn = DriverManager.getConnection(igniteJdbcConnStr);

        try (Statement stmt = conn.createStatement()) {
            prepareSchema(cleanupAllData, stmt);

            stmt.execute("SET STREAMING 1 BATCH_SIZE 100 "
                + " ALLOW_OVERWRITE " + (overwriteEntries ? 1 : 0)
                + " PER_NODE_BUFFER_SIZE 1000 "
                + " FLUSH_FREQUENCY 10000;");

            populateCachePreparedStmt = conn.prepareStatement(
                "insert into CACHE_TRACES(" +
                    "id, glowroot_tx_id, glowroot_tx_start_time, duration_nanos, offset_nanos, cache_name, operation," +
                    " args) " +
                    "values (?, ?, ?, ?, ?, ?, ?, ?)");

            populateCacheQryPreparedStmt = conn.prepareStatement(
                "insert into CACHE_QUERY_TRACES(" +
                    "id, glowroot_tx_id, glowroot_tx_start_time, duration_nanos, offset_nanos, cache_name, query) " +
                    "values (?, ?, ?, ?, ?, ?, ?)");

            populateComputePreparedStmt = conn.prepareStatement(
                "insert into COMPUTE_TRACES(" +
                    "id, glowroot_tx_id, glowroot_tx_start_time, duration_nanos, offset_nanos, task) " +
                    "values (?, ?, ?, ?, ?, ?)");

            populateTxCommitPreparedStmt = conn.prepareStatement(
                "insert into TX_COMMIT_TRACES(" +
                    "id, glowroot_tx_id, glowroot_tx_start_time, duration_nanos, offset_nanos, label) " +
                    "values (?, ?, ?, ?, ?, ?)");

            populateCacheCfgPreparedStmt = conn.prepareStatement(
                "insert into CACHE_CONFIGURATIONS(" +
                    "id, cache_name, config) " +
                    "values (?, ?, ?)");
        }
    }

    /**
     * Persist trace items to Ignite.
     *
     * @param traceItems Glowroot trace items.
     */
    public void persist(List<TraceItem> traceItems) {
        assert traceItems != null;
        assert populateCacheQryPreparedStmt != null;
        assert populateCachePreparedStmt != null;

        for (TraceItem traceItem : traceItems) {
            if (traceItem instanceof CacheTraceItem) {
                CacheTraceItem cacheTraceItem = (CacheTraceItem)traceItem;

                try {
                    populateCachePreparedStmt.setObject(1, UUID.randomUUID());

                    populateCachePreparedStmt.setObject(2, cacheTraceItem.glowrootTransaction().id());

                    populateCachePreparedStmt.setLong(3,
                        cacheTraceItem.glowrootTransaction().startTime());

                    populateCachePreparedStmt.setLong(4, cacheTraceItem.durationNanos());

                    populateCachePreparedStmt.setLong(5, cacheTraceItem.offsetNanos());

                    populateCachePreparedStmt.setString(6, cacheTraceItem.cacheName());

                    populateCachePreparedStmt.setString(7, cacheTraceItem.operation());

                    populateCachePreparedStmt.setString(8, cacheTraceItem.args());

                    populateCachePreparedStmt.executeUpdate();
                }
                catch (SQLException e) {
                    logger.log(Level.WARNING, "Unable to persist traceItem=[" + traceItem + ']', e);
                }
            }
            else if (traceItem instanceof CacheQueryTraceItem) {
                CacheQueryTraceItem cacheQryTraceItem = (CacheQueryTraceItem)traceItem;

                try {
                    populateCacheQryPreparedStmt.setObject(1, UUID.randomUUID());

                    populateCacheQryPreparedStmt.setObject(2,
                        cacheQryTraceItem.glowrootTransaction().id());

                    populateCacheQryPreparedStmt.setLong(3,
                        cacheQryTraceItem.glowrootTransaction().startTime());

                    populateCacheQryPreparedStmt.setLong(4, cacheQryTraceItem.durationNanos());

                    populateCacheQryPreparedStmt.setLong(5, cacheQryTraceItem.offsetNanos());

                    populateCacheQryPreparedStmt.setString(6, cacheQryTraceItem.cacheName());

                    populateCacheQryPreparedStmt.setString(7, cacheQryTraceItem.query());

                    populateCacheQryPreparedStmt.executeUpdate();
                }
                catch (SQLException e) {
                    logger.log(Level.WARNING, "Unable to persist traceItem=[" + traceItem + ']', e);
                }
            }
            else if (traceItem instanceof ComputeTraceItem) {
                ComputeTraceItem computeTraceItem = (ComputeTraceItem)traceItem;

                try {
                    populateComputePreparedStmt.setObject(1, UUID.randomUUID());

                    populateComputePreparedStmt.setObject(2, computeTraceItem.glowrootTransaction().id());

                    populateComputePreparedStmt.setLong(3,
                        computeTraceItem.glowrootTransaction().startTime());

                    populateComputePreparedStmt.setLong(4, computeTraceItem.durationNanos());

                    populateComputePreparedStmt.setLong(5, computeTraceItem.offsetNanos());

                    populateComputePreparedStmt.setString(6, computeTraceItem.task());

                    populateComputePreparedStmt.executeUpdate();
                }
                catch (SQLException e) {
                    logger.log(Level.WARNING, "Unable to persist traceItem=[" + traceItem + ']', e);
                }
            }
            else if (traceItem instanceof CommitTraceItem) {
                CommitTraceItem commitTraceItem = (CommitTraceItem)traceItem;

                try {
                    populateTxCommitPreparedStmt.setObject(1, UUID.randomUUID());

                    populateTxCommitPreparedStmt.setObject(2, commitTraceItem.glowrootTransaction().id());

                    populateTxCommitPreparedStmt.setLong(3,
                        commitTraceItem.glowrootTransaction().startTime());

                    populateTxCommitPreparedStmt.setLong(4, commitTraceItem.durationNanos());

                    populateTxCommitPreparedStmt.setLong(5, commitTraceItem.offsetNanos());

                    populateTxCommitPreparedStmt.setString(6, commitTraceItem.label());

                    populateTxCommitPreparedStmt.executeUpdate();
                }
                catch (SQLException e) {
                    logger.log(Level.WARNING, "Unable to persist traceItem=[" + traceItem + ']', e);
                }
            }
            else
                logger.log(Level.WARNING, "Unexpected trace item type=[" + traceItem.getClass() + ']');
        }
    }

    /**
     * Persist cache configurations.
     *
     * @param cacheCfgItems Cache configuration items.
     */
    public void persistCacheConfigMeta(List<CacheConfigMeta> cacheCfgItems) {
        assert cacheCfgItems != null;
        assert populateCacheCfgPreparedStmt != null;

        for (CacheConfigMeta cacheConfigItem : cacheCfgItems) {
            try {
                populateCacheCfgPreparedStmt.setObject(1, UUID.randomUUID());

                populateCacheCfgPreparedStmt.setString(2, cacheConfigItem.cacheName());

                populateCacheCfgPreparedStmt.setString(3, cacheConfigItem.config());

                populateCacheCfgPreparedStmt.executeUpdate();
            }
            catch (SQLException e) {
                logger.log(Level.WARNING, "Unable to persist cacheConfigItem=[" + cacheConfigItem + ']', e);
            }
        }
    }

    /**
     * Closes corresponding statesments and connections.
     * @throws Exception If failed.
     */
    @Override public void close() throws Exception {
        if (populateCachePreparedStmt != null)
            populateCachePreparedStmt.close();

        if (populateCacheQryPreparedStmt != null)
            populateCacheQryPreparedStmt.close();

        if (populateComputePreparedStmt != null)
            populateComputePreparedStmt.close();

        if (populateTxCommitPreparedStmt != null)
            populateTxCommitPreparedStmt.close();

        if (populateCacheCfgPreparedStmt != null)
            populateCacheCfgPreparedStmt.close();

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("SET STREAMING OFF;");
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, "Unable to set streaming off", e);
        }

        if (conn != null)
            conn.close();
    }

    /**
     * Prepares schema.
     *
     * @param cleanupAllData Boolean param that forses all data and schema cleanup as initial step.
     * @param igniteJdbcStmt Ignite jdbc thin connection string.
     * @throws SQLException If Failed.
     */
    private static void prepareSchema(boolean cleanupAllData, Statement igniteJdbcStmt) throws SQLException {
        if (cleanupAllData) {
            igniteJdbcStmt.executeQuery(
                "drop table if exists CACHE_TRACES;" +
                    "drop table if exists CACHE_QUERY_TRACES;" +
                    "drop table if exists COMPUTE_TRACES;" +
                    "drop table if exists TX_COMMIT_TRACES;" +
                    "drop table if exists CACHE_CONFIGURATIONS");
        }

        // TODO: 07.10.19 Do we really need id?
        // TODO: 07.10.19 Use more accurate varchar size where possible.
        igniteJdbcStmt.execute(
            "create table if not exists CACHE_TRACES"
                + "  (id                        UUID PRIMARY KEY,"
                + "   glowroot_tx_id            UUID,"
                + "   glowroot_tx_start_time    BIGINT,"
                + "   duration_nanos            BIGINT,"
                + "   offset_nanos              BIGINT,"
                + "   cache_name                VARCHAR,"
                + "   operation                 VARCHAR,"
                + "   args                      VARCHAR)");

        igniteJdbcStmt.execute(
            "create table if not exists CACHE_QUERY_TRACES"
                + "  (id                        UUID PRIMARY KEY,"
                + "   glowroot_tx_id            UUID,"
                + "   glowroot_tx_start_time    BIGINT,"
                + "   duration_nanos            BIGINT,"
                + "   offset_nanos              BIGINT,"
                + "   cache_name                VARCHAR,"
                + "   query                     VARCHAR)");

        igniteJdbcStmt.execute(
            "create table if not exists COMPUTE_TRACES"
                + "  (id                        UUID PRIMARY KEY,"
                + "   glowroot_tx_id            UUID,"
                + "   glowroot_tx_start_time    BIGINT,"
                + "   duration_nanos            BIGINT,"
                + "   offset_nanos              BIGINT,"
                + "   task                      VARCHAR)");

        igniteJdbcStmt.execute(
            "create table if not exists TX_COMMIT_TRACES"
                + "  (id                        UUID PRIMARY KEY,"
                + "   glowroot_tx_id            UUID,"
                + "   glowroot_tx_start_time    BIGINT,"
                + "   duration_nanos            BIGINT,"
                + "   offset_nanos              BIGINT,"
                + "   label                     VARCHAR)");

        igniteJdbcStmt.execute(
            "create table if not exists CACHE_CONFIGURATIONS"
                + "  (id                        UUID PRIMARY KEY,"
                + "   cache_name                VARCHAR,"
                + "   config                    VARCHAR,)");

        // TODO: 04.10.19 Indexes.
    }
}
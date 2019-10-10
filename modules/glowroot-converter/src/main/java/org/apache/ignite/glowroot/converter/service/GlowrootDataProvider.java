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

import org.apache.ignite.glowroot.converter.GlowrootUtils;
import org.apache.ignite.glowroot.converter.model.GlowrootTransactionMeta;
import org.apache.ignite.glowroot.converter.model.TraceItem;
import org.glowroot.agent.embedded.util.CappedDatabase;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.glowroot.wire.api.model.TraceOuterClass.Trace.Entry.*;
import static org.glowroot.wire.api.model.TraceOuterClass.Trace.Entry.parser;

/**
 * Class that provide trace data from glowroot storages.
 */
public class GlowrootDataProvider implements AutoCloseable {
    /** **/
    private static final Logger logger = Logger.getLogger(GlowrootDataProvider.class.getName());

    /** Glowroot transactions result set with all glowroot transactions. **/
    private final ResultSet transactionsRS;

    /** Part of glowroot api to work with inner database that contains trace data. **/
    private final CappedDatabase traceCappedDatabase;

    /** Total glowroot transactions count. **/
    private final long totalTxCnt;

    /**
     * Establish connections to glowroot databases: h2 database that contains glowroot transactions metadata and
     * trace-detail capped database. Besides that given method retrieves glowroot transactions count and transactions
     * themself.
     *
     * @param dataFolderPath path to glowroot data folder.
     * @throws SQLException If failed to esablish connection to glowroot h2 database or failed to retrieve
     * transactions count or transactions themself.
     * @throws IOException If failed to esablish connection to glowroot trace-detail capped database.
     */
    public GlowrootDataProvider(String dataFolderPath) throws SQLException, IOException {
        String glowrootDataFilePath = dataFolderPath + "data.h2.db";

        Connection glowrootConn = null;

        glowrootConn = GlowrootUtils.createConnection(new File(glowrootDataFilePath));

        ResultSet rsCnt = glowrootConn.createStatement().executeQuery(
            "Select count(*) from Trace where TRANSACTION_TYPE = 'Ignite';");

        rsCnt.next();

        totalTxCnt = rsCnt.getLong(1);

        transactionsRS = glowrootConn.createStatement().executeQuery(
            "Select ID, START_TIME, DURATION_NANOS, ENTRIES_CAPPED_ID from Trace where TRANSACTION_TYPE = " +
                "'Ignite';");

        traceCappedDatabase = new CappedDatabase(
            new File(dataFolderPath + "trace-detail.capped.db"), 1,
            Executors.newSingleThreadScheduledExecutor(), com.google.common.base.Ticker.systemTicker().systemTicker());
    }

    /**
     * Moves cursor to next glowroot transaction.
     *
     * @return {@code True} if there are more transactions, {@code False} otherwise.
     * @throws SQLException If failed to retrieve next glowroot transaction.
     */
    public boolean next() throws SQLException {
        return transactionsRS.next();
    }

    /**
     * Reads and parses next glowroot transaction and all corresponding traces.
     *
     * @return All traces of a glowroot transaction converted to corresponding model instances.
     */
    @SuppressWarnings("unchecked")
    public List<TraceItem> readTraceData() {
        GlowrootTransactionMeta txMeta;

        long cappedId;

        try {
            txMeta = new GlowrootTransactionMeta(
                transactionsRS.getString(1),
                transactionsRS.getLong(2),
                transactionsRS.getLong(3));

            cappedId = transactionsRS.getLong(4);
        }
        catch (SQLException e) {
            logger.log(Level.WARNING, "Unable to retrieve transaction. ", e);

            return Collections.emptyList();
        }

        try {
            return traceCappedDatabase.readMessages(cappedId, parser()).stream().map(
                traceEntry -> DataParser.parse(txMeta, traceEntry.getDurationNanos(), traceEntry.getStartOffsetNanos(),
                    traceEntry.getMessage())).collect(Collectors.toList());
        }
        catch (IOException e) {
            logger.log(Level.WARNING, "Unable to retrieve traces for transaction with id=" + txMeta.id(), e);

            return Collections.emptyList();
        }
    }

    /**
     * Closes connections to h2 and trace-detail databases.
     * @throws Exception If failed.
     */
    @Override public void close() throws Exception {
        if (transactionsRS != null)
            transactionsRS.close();

        if (traceCappedDatabase != null)
            traceCappedDatabase.close();
    }

    /**
     * @return Total glowroot transactions count.
     */
    public long getTotalTxCnt() {
        return totalTxCnt;
    }
}

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

package org.apache.ignite.glowroot.converter.service;import org.apache.ignite.glowroot.converter.GlowrootUtils;
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

public class GlowrootDataProvider implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(GlowrootDataProvider.class.getName());

    private final ResultSet transactionsRS;

    private final CappedDatabase traceCappedDatabase;

    private final long totalTxCnt;

    public GlowrootDataProvider(String dataFolderPath) throws SQLException, IOException {
        String glowrootDataFilePath = dataFolderPath + "data.h2.db";

        Connection glowrootConn = null;

        glowrootConn = GlowrootUtils.createConnection(new File(glowrootDataFilePath));

        ResultSet rsCnt = glowrootConn.createStatement().executeQuery("Select count(*) from Trace where TRANSACTION_TYPE = 'Ignite';");

        rsCnt.next();

        totalTxCnt = rsCnt.getLong(1);

        transactionsRS = glowrootConn.createStatement().executeQuery("Select ID, START_TIME, DURATION_NANOS, ENTRIES_CAPPED_ID from Trace where TRANSACTION_TYPE = 'Ignite';");

        traceCappedDatabase = new CappedDatabase(new File(dataFolderPath + "trace-detail.capped.db"), 1, Executors.newSingleThreadScheduledExecutor(), com.google.common.base.Ticker.systemTicker().systemTicker());
    }

    public boolean next() throws SQLException {
        return transactionsRS.next();
    }

    @SuppressWarnings("unchecked")
    public List<TraceItem> readTraceData() throws IOException {
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

            return Collections.EMPTY_LIST;
        }

        try {
            return traceCappedDatabase.readMessages(cappedId, parser()).stream().map(
                traceEntry -> DataParser.parse(txMeta, traceEntry.getDurationNanos(), traceEntry.getStartOffsetNanos(),
                    traceEntry.getMessage())).collect(Collectors.toList());
        }
        catch (IOException e) {
            logger.log(Level.WARNING, "Unable to retrieve traces for transaction with id=" + txMeta.id(), e);

            return Collections.EMPTY_LIST;
        }
    }

    @Override public void close() throws Exception {
        if (transactionsRS != null)
            transactionsRS.close();

        if (traceCappedDatabase != null)
            traceCappedDatabase.close();
    }

    public long getTotalTxCnt() {
        return totalTxCnt;
    }
}

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

package org.apache.ignite.internal.processors.io;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.BulkLoadContextCursor;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.bulkload.BulkLoadAckClientParameters;
import org.apache.ignite.internal.processors.bulkload.BulkLoadCacheWriter;
import org.apache.ignite.internal.processors.bulkload.BulkLoadCsvFormat;
import org.apache.ignite.internal.processors.bulkload.BulkLoadLocationFile;
import org.apache.ignite.internal.processors.bulkload.BulkLoadLocationTable;
import org.apache.ignite.internal.processors.bulkload.BulkLoadParser;
import org.apache.ignite.internal.processors.bulkload.BulkLoadProcessor;
import org.apache.ignite.internal.processors.bulkload.BulkLoadStreamerWriter;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.dml.DmlBulkLoadDataConverter;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlan;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlanBuilder;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;
import org.apache.ignite.internal.util.lang.IgniteClosureX;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * This implementation of BulkLoadCommandProcessor is for JDBC thin client only.
 * Client-side CSV importing is supported only by JDBC thin client.
 */
public class BasicBulkLoadCommandProcessor implements BulkLoadCommandProcessor {

    @Override public FieldsQueryCursor<List<?>> processBulkLoadCommand(
        GridKernalContext ctx,
        SqlBulkLoadCommand cmd,
        Long qryId) throws IgniteCheckedException {

        if (!(cmd.from() instanceof BulkLoadLocationFile
                && cmd.into() instanceof BulkLoadLocationTable
                && cmd.format() instanceof BulkLoadCsvFormat))
            throw new IgniteSQLException("Community edition supports only COPY FROM <file> INTO <table> FORMAT CSV",
                    IgniteQueryErrorCode.UNEXPECTED_ELEMENT_TYPE);

        if (cmd.packetSize() == null)
            cmd.packetSize(BulkLoadAckClientParameters.DFLT_PACKET_SIZE);

        IgniteH2Indexing idx = (IgniteH2Indexing) ctx.query().getIndexing();
        BulkLoadLocationTable into = (BulkLoadLocationTable) cmd.into();
        GridH2Table tbl = idx.schemaManager().dataTable(into.schemaName(), into.tableName());

        if (tbl == null) {
            throw new IgniteSQLException("Table does not exist: " + into.tableName(),
                IgniteQueryErrorCode.TABLE_NOT_FOUND);
        }

        H2Utils.checkAndStartNotStartedCache(ctx, tbl);

        UpdatePlan plan = UpdatePlanBuilder.planForBulkLoad(into.columns(), tbl);

        IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> dataConverter = new DmlBulkLoadDataConverter(plan);

        IgniteDataStreamer<Object, Object> streamer = ctx.grid().dataStreamer(tbl.cacheName());

        BulkLoadCacheWriter outputWriter = new BulkLoadStreamerWriter(streamer);

        BulkLoadParser inputParser = BulkLoadParser.createParser(cmd.format());

        BulkLoadProcessor processor = new BulkLoadProcessor(inputParser, dataConverter, outputWriter,
            idx.runningQueryManager(), qryId, ctx.tracing());

        String path = ((BulkLoadLocationFile) cmd.from()).path();
        BulkLoadAckClientParameters params = new BulkLoadAckClientParameters(path, cmd.packetSize());

        return new BulkLoadContextCursor(processor, params);
    }
}

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.ComputeMXBeanImpl;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.QueryMXBeanImpl;
import org.apache.ignite.internal.ServiceMXBeanImpl;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.io.BulkLoadCommandProcessorFactory;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.NestedTxMode;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.SqlClientContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlterTableAddColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlterTableDropColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDropIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDropTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.messages.GridQueryKillRequest;
import org.apache.ignite.internal.processors.query.messages.GridQueryKillResponse;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.sql.command.SqlAlterTableCommand;
import org.apache.ignite.internal.sql.command.SqlAlterUserCommand;
import org.apache.ignite.internal.sql.command.SqlAnalyzeCommand;
import org.apache.ignite.internal.sql.command.SqlBeginTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCommitTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.apache.ignite.internal.sql.command.SqlCreateUserCommand;
import org.apache.ignite.internal.sql.command.SqlDropIndexCommand;
import org.apache.ignite.internal.sql.command.SqlDropStatisticsCommand;
import org.apache.ignite.internal.sql.command.SqlDropUserCommand;
import org.apache.ignite.internal.sql.command.SqlIndexColumn;
import org.apache.ignite.internal.sql.command.SqlKillClientCommand;
import org.apache.ignite.internal.sql.command.SqlKillComputeTaskCommand;
import org.apache.ignite.internal.sql.command.SqlKillContinuousQueryCommand;
import org.apache.ignite.internal.sql.command.SqlKillQueryCommand;
import org.apache.ignite.internal.sql.command.SqlKillScanQueryCommand;
import org.apache.ignite.internal.sql.command.SqlKillServiceCommand;
import org.apache.ignite.internal.sql.command.SqlKillTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlRefreshStatitsicsCommand;
import org.apache.ignite.internal.sql.command.SqlRollbackTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlSetStreamingCommand;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.CIX2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.gridgain.internal.h2.command.Prepared;
import org.gridgain.internal.h2.command.ddl.AlterTableAlterColumn;
import org.gridgain.internal.h2.command.ddl.CreateIndex;
import org.gridgain.internal.h2.command.ddl.CreateTable;
import org.gridgain.internal.h2.command.ddl.DropIndex;
import org.gridgain.internal.h2.command.ddl.DropTable;
import org.gridgain.internal.h2.command.dml.NoOperation;
import org.gridgain.internal.h2.table.Column;
import org.gridgain.internal.h2.value.DataType;
import org.gridgain.internal.h2.value.TypeInfo;
import org.gridgain.internal.h2.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.mvccEnabled;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.tx;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.txStart;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser.PARAM_WRAP_VALUE;

/**
 * Processor responsible for execution of all non-SELECT and non-DML commands.
 */
public class CommandProcessor {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** H2 Indexing. */
    private final IgniteH2Indexing idx;

    /** Logger. */
    private final IgniteLogger log;

    /** Is backward compatible handling of UUID through DDL enabled. */
    private static final boolean handleUuidAsByte =
            IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_SQL_UUID_DDL_BYTE_FORMAT, false);

    /** Query cancel request counter. */
    private final AtomicLong qryCancelReqCntr = new AtomicLong();

    /** Cancellation runs. */
    private ConcurrentMap<Long, KillQueryRun> cancellationRuns = new ConcurrentHashMap<>();

    /** Flag indicate that node is stopped or not. */
    private volatile boolean stopped;

    /** */
    private final ReadWriteLock killQryLock = new ReentrantReadWriteLock();

    /** KILL COMMAND support added since. */
    private static final IgniteProductVersion KILL_COMMAND_SINCE_VER = IgniteProductVersion.fromString("2.8.0");

    /** Local node message handler */
    private final CIX2<ClusterNode, Message> locNodeMsgHnd = new CIX2<ClusterNode, Message>() {
        @Override public void applyx(ClusterNode locNode, Message msg) {
            onMessage(locNode.id(), msg);
        }
    };

    private final BulkLoadCommandProcessorFactory bulkLoadCommandProcessorFactory;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param schemaMgr Schema manager.
     */
    public CommandProcessor(GridKernalContext ctx, SchemaManager schemaMgr, IgniteH2Indexing idx) {
        this.ctx = ctx;
        this.schemaMgr = schemaMgr;
        this.idx = idx;

        log = ctx.log(CommandProcessor.class);
        this.bulkLoadCommandProcessorFactory = new BulkLoadCommandProcessorFactory(ctx);
    }

    /**
     * Start executor.
     */
    public void start() {
        ctx.io().addMessageListener(GridTopic.TOPIC_QUERY, (nodeId, msg, plc) -> onMessage(nodeId, msg));

        ctx.event().addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(final Event evt) {
                UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

                List<GridFutureAdapter<String>> futs = new ArrayList<>();

                killQryLock.writeLock().lock();

                try {
                    Iterator<KillQueryRun> it = cancellationRuns.values().iterator();

                    while (it.hasNext()) {
                        KillQueryRun qryRun = it.next();

                        if (qryRun.nodeId().equals(nodeId)) {
                            futs.add(qryRun.cancelFuture());

                            it.remove();
                        }
                    }
                }
                finally {
                    killQryLock.writeLock().unlock();
                }

                futs.forEach(f -> f.onDone("Query node has left the grid: [nodeId=" + nodeId + "]"));
            }
        }, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
    }

    /**
     * Close executor.
     */
    public void stop() {
        stopped = true;

        completeCancellationFutures("Local node is stopping: [nodeId=" + ctx.localNodeId() + "]");
    }

    /**
     * Client disconnected callback.
     */
    public void onDisconnected() {
        completeCancellationFutures("Failed to cancel query because local client node has been disconnected from the cluster");
    }

    /**
     * @param err Text of error to complete futures.
     */
    private void completeCancellationFutures(@Nullable String err) {
        killQryLock.writeLock().lock();

        try {
            Iterator<KillQueryRun> it = cancellationRuns.values().iterator();

            while (it.hasNext()) {
                KillQueryRun qryRun = it.next();

                qryRun.cancelFuture().onDone(err);

                it.remove();
            }
        }
        finally {
            killQryLock.writeLock().unlock();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    public void onMessage(UUID nodeId, Object msg) {
        assert msg != null;

        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null)
            return; // Node left, ignore.

        boolean processed = true;

        if (msg instanceof GridQueryKillRequest)
            onQueryKillRequest((GridQueryKillRequest)msg, node);
        if (msg instanceof GridQueryKillResponse)
            onQueryKillResponse((GridQueryKillResponse)msg);
        else
            processed = false;

        if (processed && log.isDebugEnabled())
            log.debug("Processed response: " + nodeId + "->" + ctx.localNodeId() + " " + msg);
    }

    /**
     * Process request to kill query.
     *
     * @param msg Message.
     * @param node Cluster node.
     */
    private void onQueryKillRequest(GridQueryKillRequest msg, ClusterNode node) {
        final long qryId = msg.nodeQryId();

        String err = null;

        GridRunningQueryInfo runningQryInfo = idx.runningQueryManager().runningQueryInfo(qryId);

        if (runningQryInfo == null)
            err = "Query with provided ID doesn't exist " +
                "[nodeId=" + ctx.localNodeId() + ", qryId=" + qryId + "]";
        else if (!runningQryInfo.cancelable())
            err = "Query doesn't support cancellation " +
                "[nodeId=" + ctx.localNodeId() + ", qryId=" + qryId + "]";

        if (msg.asyncResponse() || err != null)
            sendKillResponse(msg, node, err);

        if (err == null) {
            try {
                runningQryInfo.cancel();
            }
            catch (Exception e) {
                U.warn(log, "Cancellation of query failed: [qryId=" + qryId + "]", e);

                if (!msg.asyncResponse())
                    sendKillResponse(msg, node, e.getMessage());

                return;
            }

            if (!msg.asyncResponse())
                runningQryInfo.runningFuture().listen((f) -> sendKillResponse(msg, node, f.result()));
        }
    }

    /**
     * @param msg Kill request message.
     * @param node Initial kill request node.
     * @param err Error message
     */
    private void sendKillResponse(GridQueryKillRequest msg, ClusterNode node, @Nullable String err) {
        boolean snd = idx.send(GridTopic.TOPIC_QUERY,
            GridTopic.TOPIC_QUERY.ordinal(),
            Collections.singleton(node),
            new GridQueryKillResponse(msg.requestId(), err),
            null,
            locNodeMsgHnd,
            GridIoPolicy.MANAGEMENT_POOL,
            false);

        if (!snd)
            U.warn(log, "Resposne on query cancellation wasn't send back: [qryId=" + msg.nodeQryId() + "]");
    }

    /**
     * Process response to kill query request.
     *
     * @param msg Message.
     */
    private void onQueryKillResponse(GridQueryKillResponse msg) {
        KillQueryRun qryRun;

        killQryLock.readLock().lock();

        try {
            qryRun = cancellationRuns.remove(msg.requestId());
        }
        finally {
            killQryLock.readLock().unlock();
        }

        if (qryRun != null)
            qryRun.cancelFuture().onDone(msg.error());
    }

    /**
     * @param cmd Command.
     * @return {@code True} if this is supported DDL command.
     */
    private static boolean isDdl(SqlCommand cmd) {
        return cmd instanceof SqlCreateIndexCommand
            || cmd instanceof SqlDropIndexCommand
            || cmd instanceof SqlAlterTableCommand
            || cmd instanceof SqlCreateUserCommand
            || cmd instanceof SqlAlterUserCommand
            || cmd instanceof SqlDropUserCommand
            || cmd instanceof SqlAnalyzeCommand
            || cmd instanceof SqlRefreshStatitsicsCommand
            || cmd instanceof SqlDropStatisticsCommand;
    }

    /**
     * Execute command.
     *
     * @param sql SQL.
     * @param cmdNative Native command (if any).
     * @param cmdH2 H2 command (if any).
     * @param params Parameters.
     * @param cliCtx Client context.
     * @param qryId Running query ID.
     * @return Result.
     */
    public CommandResult runCommand(String sql, SqlCommand cmdNative, GridSqlStatement cmdH2,
        QueryParameters params, @Nullable SqlClientContext cliCtx, Long qryId) throws IgniteCheckedException {
        assert cmdNative != null || cmdH2 != null;

        // Do execute.
        FieldsQueryCursor<List<?>> res = H2Utils.zeroCursor();
        boolean unregister = true;

        if (cmdNative != null) {
            assert cmdH2 == null;

            if (isDdl(cmdNative))
                runCommandNativeDdl(sql, cmdNative);
            else if (cmdNative instanceof SqlBulkLoadCommand) {
                // SqlClientContext is not available if using JDBC Client Driver but
                // serverBulkLoadEnabled should be still enabled by default.
                boolean serverBulkLoadEnabled = cliCtx == null || cliCtx.serverBulkLoadEnabled();
                res = bulkLoadCommandProcessorFactory
                    .getBulkLoadCommandProcessor(serverBulkLoadEnabled)
                    .processBulkLoadCommand(ctx, (SqlBulkLoadCommand) cmdNative, qryId);

                unregister = false;
            }
            else if (cmdNative instanceof SqlSetStreamingCommand)
                processSetStreamingCommand((SqlSetStreamingCommand)cmdNative, cliCtx);
            else if (cmdNative instanceof SqlKillQueryCommand)
                processKillQueryCommand((SqlKillQueryCommand) cmdNative);
            else if (cmdNative instanceof SqlKillContinuousQueryCommand)
                processKillContinuousQueryCommand((SqlKillContinuousQueryCommand) cmdNative);
            else if (cmdNative instanceof SqlKillScanQueryCommand)
                processKillScanQueryCommand((SqlKillScanQueryCommand) cmdNative);
            else if (cmdNative instanceof SqlKillClientCommand)
                processKillClientCommand((SqlKillClientCommand)cmdNative);
            else if (cmdNative instanceof SqlKillComputeTaskCommand)
                processKillComputeTaskCommand((SqlKillComputeTaskCommand) cmdNative);
            else if (cmdNative instanceof SqlKillServiceCommand)
                processKillServiceTaskCommand((SqlKillServiceCommand) cmdNative);
            else if (cmdNative instanceof SqlKillTransactionCommand)
                processKillTxCommand((SqlKillTransactionCommand) cmdNative);
            else
                processTxCommand(cmdNative, params);
        }
        else {
            assert cmdH2 != null;

            runCommandH2(sql, cmdH2);
        }

        return new CommandResult(res, unregister);
    }

    /**
     * Process kill query command
     *
     * @param cmd Command.
     */
    private void processKillQueryCommand(SqlKillQueryCommand cmd) {
        ctx.security().authorize(SecurityPermission.KILL_QUERY);

        GridFutureAdapter<String> fut = new GridFutureAdapter<>();

        killQryLock.readLock().lock();

        try {
            if (stopped)
                throw new IgniteSQLException("Failed to cancel query due to node is stopped [nodeId=" + cmd.nodeId() +
                    ",qryId=" + cmd.nodeQueryId() + "]");

            ClusterNode node = ctx.discovery().node(cmd.nodeId());

            if (node != null) {
                if (node.version().compareTo(KILL_COMMAND_SINCE_VER) < 0)
                    throw new IgniteSQLException("Failed to cancel query: KILL QUERY operation is supported in " +
                        "versions 2.8.0 and newer");

                if (log.isInfoEnabled())
                    log.info("Start to process query cancel: [nodeId=" + cmd.nodeId() + ", qryId=" + cmd.nodeQueryId() + ']');

                KillQueryRun qryRun = new KillQueryRun(cmd.nodeId(), cmd.nodeQueryId(), fut);

                long reqId = qryCancelReqCntr.incrementAndGet();

                cancellationRuns.put(reqId, qryRun);

                boolean snd = idx.send(GridTopic.TOPIC_QUERY,
                    GridTopic.TOPIC_QUERY.ordinal(),
                    Collections.singleton(node),
                    new GridQueryKillRequest(reqId, cmd.nodeQueryId(), cmd.async()),
                    null,
                    locNodeMsgHnd,
                    GridIoPolicy.MANAGEMENT_POOL,
                    cmd.async()
                );

                if (!snd) {
                    cancellationRuns.remove(reqId);

                    throw new IgniteSQLException("Failed to cancel query due communication problem " +
                        "[nodeId=" + cmd.nodeId() + ",qryId=" + cmd.nodeQueryId() + "]");
                }
            }
            else
                throw new IgniteSQLException("Failed to cancel query, node is not alive [nodeId=" + cmd.nodeId() + ",qryId="
                    + cmd.nodeQueryId() + "]");
        }
        finally {
            killQryLock.readLock().unlock();
        }

        try {
            String err = fut.get();

            if (err != null)
                throw new IgniteSQLException("Failed to cancel query [nodeId=" + cmd.nodeId() + ",qryId="
                    + cmd.nodeQueryId() + ",err=" + err + "]");
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSQLException("Failed to cancel query [nodeId=" + cmd.nodeId() + ",qryId="
                + cmd.nodeQueryId() + ",err=" + e + "]", e);
        }
    }

    /**
     * Process analyze command.
     *
     * @param cmd Sql analyze command.
     */
    private void processAnalyzeCommand(SqlAnalyzeCommand cmd) throws IgniteCheckedException {
        ctx.security().authorize(SecurityPermission.CHANGE_STATISTICS);

        IgniteH2Indexing indexing = (IgniteH2Indexing)ctx.query().getIndexing();

        StatisticsObjectConfiguration objCfgs[] = cmd.configurations().stream()
            .map(t -> {
                if (t.key().schema() == null) {
                    StatisticsKey key = new StatisticsKey(cmd.schemaName(), t.key().obj());

                    return new StatisticsObjectConfiguration(key, t.columns().values(),
                        t.maxPartitionObsolescencePercent());
                }
                else
                    return t;
            }).toArray(StatisticsObjectConfiguration[]::new);

        indexing.statsManager().collectStatistics(objCfgs);
    }

    /**
     * Process refresh statistics command.
     *
     * @param cmd Refresh statistics command.
     */
    private void processRefreshStatisticsCommand(SqlRefreshStatitsicsCommand cmd) throws IgniteCheckedException {
        ctx.security().authorize(SecurityPermission.REFRESH_STATISTICS);

        IgniteH2Indexing indexing = (IgniteH2Indexing)ctx.query().getIndexing();

        StatisticsTarget[] targets = cmd.targets().stream()
            .map(t -> (t.schema() == null) ? new StatisticsTarget(cmd.schemaName(), t.obj(), t.columns()) : t)
            .toArray(StatisticsTarget[]::new);

        indexing.statsManager().refreshStatistics(targets);
    }

    /**
     * Process drop statistics command.
     *
     * @param cmd Drop statistics command.
     */
    private void processDropStatisticsCommand(SqlDropStatisticsCommand cmd) throws IgniteCheckedException {
        ctx.security().authorize(SecurityPermission.CHANGE_STATISTICS);

        IgniteH2Indexing indexing = (IgniteH2Indexing)ctx.query().getIndexing();

        StatisticsTarget[] targets = cmd.targets().stream()
            .map(t -> (t.schema() == null) ? new StatisticsTarget(cmd.schemaName(), t.obj(), t.columns()) : t)
            .toArray(StatisticsTarget[]::new);

        indexing.statsManager().dropStatistics(targets);
    }

    /**
     * Process kill continuous query cmd.
     *
     * @param cmd Command.
     */
    private void processKillContinuousQueryCommand(SqlKillContinuousQueryCommand cmd) {
        new QueryMXBeanImpl(ctx).cancelContinuous(cmd.getOriginNodeId(), cmd.getRoutineId());
    }

    /**
     * Process kill scan query command.
     *
     * @param command Command.
     */
    private void processKillScanQueryCommand(SqlKillScanQueryCommand command) {
        new QueryMXBeanImpl(ctx)
            .cancelScan(command.getOriginNodeId(), command.getCacheName(), command.getQryId());
    }

    /**
     * Process kill client command.
     *
     * @param cmd Command.
     */
    private void processKillClientCommand(SqlKillClientCommand cmd) {
        if (cmd.connectionId() == null)
            ctx.sqlListener().mxBean().dropAllConnections();
        else
            ctx.sqlListener().mxBean().dropConnection(cmd.connectionId());
    }

    /**
     * Process kill compute task command.
     *
     * @param command Command.
     */
    private void processKillComputeTaskCommand(SqlKillComputeTaskCommand command) {
        new ComputeMXBeanImpl(ctx).cancel(command.getSessionId());
    }

    /**
     * Process kill transaction command.
     *
     * @param command Command.
     */
    private void processKillTxCommand(SqlKillTransactionCommand command) {
        new TransactionsMXBeanImpl(ctx).cancel(command.getXid());
    }

    /**
     * Process kill service command.
     *
     * @param cmd Command.
     */
    private void processKillServiceTaskCommand(SqlKillServiceCommand cmd) {
        new ServiceMXBeanImpl(ctx).cancel(cmd.getName());
    }

    /**
     * Run DDL statement.
     *
     * @param sql Original SQL.
     * @param cmd Command.
     */
    private void runCommandNativeDdl(String sql, SqlCommand cmd) {
        IgniteInternalFuture fut = null;

        try {
            isDdlOnSchemaSupported(cmd.schemaName());

            finishActiveTxIfNecessary();

            if (cmd instanceof SqlCreateIndexCommand) {
                SqlCreateIndexCommand cmd0 = (SqlCreateIndexCommand)cmd;

                GridH2Table tbl = schemaMgr.dataTable(cmd0.schemaName(), cmd0.tableName());

                if (tbl == null)
                    throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, cmd0.tableName());

                assert tbl.rowDescriptor() != null;

                ensureDdlSupported(tbl);

                QueryIndex newIdx = new QueryIndex();

                newIdx.setName(cmd0.indexName());

                newIdx.setIndexType(cmd0.spatial() ? QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED);

                LinkedHashMap<String, Boolean> flds = new LinkedHashMap<>();

                // Let's replace H2's table and property names by those operated by GridQueryProcessor.
                GridQueryTypeDescriptor typeDesc = tbl.rowDescriptor().type();

                for (SqlIndexColumn col : cmd0.columns()) {
                    GridQueryProperty prop = typeDesc.property(col.name());

                    if (prop == null)
                        throw new SchemaOperationException(SchemaOperationException.CODE_COLUMN_NOT_FOUND, col.name());

                    flds.put(prop.name(), !col.descending());
                }

                newIdx.setFields(flds);
                newIdx.setInlineSize(cmd0.inlineSize());

                fut = ctx.query().dynamicIndexCreate(tbl.cacheName(), cmd.schemaName(), typeDesc.tableName(),
                    newIdx, cmd0.ifNotExists(), cmd0.parallel());
            }
            else if (cmd instanceof SqlDropIndexCommand) {
                SqlDropIndexCommand cmd0 = (SqlDropIndexCommand)cmd;

                GridH2Table tbl = schemaMgr.dataTableForIndex(cmd0.schemaName(), cmd0.indexName());

                if (tbl != null) {
                    ensureDdlSupported(tbl);

                    fut = ctx.query().dynamicIndexDrop(tbl.cacheName(), cmd0.schemaName(), cmd0.indexName(),
                        cmd0.ifExists());
                }
                else {
                    if (cmd0.ifExists())
                        fut = new GridFinishedFuture();
                    else
                        throw new SchemaOperationException(SchemaOperationException.CODE_INDEX_NOT_FOUND,
                            cmd0.indexName());
                }
            }
            else if (cmd instanceof SqlAlterTableCommand) {
                SqlAlterTableCommand cmd0 = (SqlAlterTableCommand)cmd;

                GridH2Table tbl = schemaMgr.dataTable(cmd0.schemaName(), cmd0.tableName());

                if (tbl == null) {
                    throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND,
                        cmd0.tableName());
                }

                Boolean logging = cmd0.logging();

                assert logging != null : "Only LOGGING/NOLOGGING are supported at the moment.";

                IgniteCluster cluster = ctx.grid().cluster();

                if (logging) {
                    boolean res = cluster.enableWal(tbl.cacheName());

                    if (!res)
                        throw new IgniteSQLException("Logging already enabled for table: " + cmd0.tableName());
                }
                else {
                    boolean res = cluster.disableWal(tbl.cacheName());

                    if (!res)
                        throw new IgniteSQLException("Logging already disabled for table: " + cmd0.tableName());
                }

                fut = new GridFinishedFuture();
            }
            else if (cmd instanceof SqlCreateUserCommand) {
                SqlCreateUserCommand addCmd = (SqlCreateUserCommand)cmd;

                ctx.authentication().addUser(addCmd.userName(), addCmd.password());
            }
            else if (cmd instanceof SqlAlterUserCommand) {
                SqlAlterUserCommand altCmd = (SqlAlterUserCommand)cmd;

                ctx.authentication().updateUser(altCmd.userName(), altCmd.password());
            }
            else if (cmd instanceof SqlDropUserCommand) {
                SqlDropUserCommand dropCmd = (SqlDropUserCommand)cmd;

                ctx.authentication().removeUser(dropCmd.userName());
            }
            else if (cmd instanceof SqlAnalyzeCommand)
                processAnalyzeCommand((SqlAnalyzeCommand) cmd);
            else if (cmd instanceof SqlRefreshStatitsicsCommand)
                processRefreshStatisticsCommand((SqlRefreshStatitsicsCommand) cmd);
            else if (cmd instanceof SqlDropStatisticsCommand)
                processDropStatisticsCommand((SqlDropStatisticsCommand) cmd);
            else
                throw new IgniteSQLException("Unsupported DDL operation: " + sql,
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

            if (fut != null)
                fut.get();
        }
        catch (SchemaOperationException e) {
            throw convert(e);
        }
        catch (IgniteSQLException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteSQLException(e.getMessage(), e);
        }
    }

    /**
     * Execute DDL statement.
     *
     * @param sql SQL.
     * @param cmdH2 Command.
     */
    private void runCommandH2(String sql, GridSqlStatement cmdH2) {
        IgniteInternalFuture fut = null;

        try {
            finishActiveTxIfNecessary();

            if (cmdH2 instanceof GridSqlCreateIndex) {
                GridSqlCreateIndex cmd = (GridSqlCreateIndex)cmdH2;

                isDdlOnSchemaSupported(cmd.schemaName());

                GridH2Table tbl = schemaMgr.dataTable(cmd.schemaName(), cmd.tableName());

                if (tbl == null)
                    throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, cmd.tableName());

                assert tbl.rowDescriptor() != null;

                ensureDdlSupported(tbl);

                QueryIndex newIdx = new QueryIndex();

                newIdx.setName(cmd.index().getName());

                newIdx.setIndexType(cmd.index().getIndexType());

                LinkedHashMap<String, Boolean> flds = new LinkedHashMap<>();

                // Let's replace H2's table and property names by those operated by GridQueryProcessor.
                GridQueryTypeDescriptor typeDesc = tbl.rowDescriptor().type();

                for (Map.Entry<String, Boolean> e : cmd.index().getFields().entrySet()) {
                    GridQueryProperty prop = typeDesc.property(e.getKey());

                    if (prop == null)
                        throw new SchemaOperationException(SchemaOperationException.CODE_COLUMN_NOT_FOUND, e.getKey());

                    flds.put(prop.name(), e.getValue());
                }

                newIdx.setFields(flds);

                fut = ctx.query().dynamicIndexCreate(tbl.cacheName(), cmd.schemaName(), typeDesc.tableName(),
                    newIdx, cmd.ifNotExists(), 0);
            }
            else if (cmdH2 instanceof GridSqlDropIndex) {
                GridSqlDropIndex cmd = (GridSqlDropIndex) cmdH2;

                isDdlOnSchemaSupported(cmd.schemaName());

                GridH2Table tbl = schemaMgr.dataTableForIndex(cmd.schemaName(), cmd.indexName());

                if (tbl != null) {
                    ensureDdlSupported(tbl);

                    fut = ctx.query().dynamicIndexDrop(tbl.cacheName(), cmd.schemaName(), cmd.indexName(),
                        cmd.ifExists());
                }
                else {
                    if (cmd.ifExists())
                        fut = new GridFinishedFuture();
                    else
                        throw new SchemaOperationException(SchemaOperationException.CODE_INDEX_NOT_FOUND,
                            cmd.indexName());
                }
            }
            else if (cmdH2 instanceof GridSqlCreateTable) {
                GridSqlCreateTable cmd = (GridSqlCreateTable)cmdH2;

                ctx.security().authorize(cmd.cacheName(), SecurityPermission.CACHE_CREATE);

                isDdlOnSchemaSupported(cmd.schemaName());

                GridH2Table tbl = schemaMgr.dataTable(cmd.schemaName(), cmd.tableName());

                if (tbl != null) {
                    if (!cmd.ifNotExists())
                        throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_EXISTS,
                            cmd.tableName());
                }
                else {
                    QueryEntity e = toQueryEntity(ctx, cmd);

                    CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(cmd.tableName());

                    ccfg.setQueryEntities(Collections.singleton(e));
                    ccfg.setSqlSchema(cmd.schemaName());

                    SchemaOperationException err =
                        QueryUtils.checkQueryEntityConflicts(ccfg, ctx.cache().cacheDescriptors().values());

                    if (err != null)
                        throw err;

                    if (!F.isEmpty(cmd.cacheName()) && ctx.cache().cacheDescriptor(cmd.cacheName()) != null) {
                        ctx.query().dynamicAddQueryEntity(
                                cmd.cacheName(),
                                cmd.schemaName(),
                                e,
                                cmd.parallelism(),
                                true
                        ).get();
                    }
                    else {
                        ctx.query().dynamicTableCreate(
                            cmd.schemaName(),
                            e,
                            cmd.templateName(),
                            cmd.cacheName(),
                            cmd.cacheGroup(),
                            cmd.dataRegionName(),
                            cmd.affinityKey(),
                            cmd.atomicityMode(),
                            cmd.writeSynchronizationMode(),
                            cmd.backups(),
                            cmd.ifNotExists(),
                            cmd.encrypted(),
                            cmd.parallelism()
                        );
                    }
                }
            }
            else if (cmdH2 instanceof GridSqlDropTable) {
                GridSqlDropTable cmd = (GridSqlDropTable)cmdH2;

                isDdlOnSchemaSupported(cmd.schemaName());

                GridH2Table tbl = schemaMgr.dataTable(cmd.schemaName(), cmd.tableName());

                if (tbl == null) {
                    if (!cmd.ifExists())
                        throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND,
                            cmd.tableName());
                }
                else {
                    ctx.security().authorize(tbl.cacheName(), SecurityPermission.CACHE_DESTROY);

                    ctx.query().dynamicTableDrop(tbl.cacheName(), cmd.tableName(), cmd.ifExists());
                }
            }
            else if (cmdH2 instanceof GridSqlAlterTableAddColumn) {
                GridSqlAlterTableAddColumn cmd = (GridSqlAlterTableAddColumn)cmdH2;

                isDdlOnSchemaSupported(cmd.schemaName());

                GridH2Table tbl = schemaMgr.dataTable(cmd.schemaName(), cmd.tableName());

                if (tbl == null) {
                    if (!cmd.ifTableExists())
                        throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND,
                            cmd.tableName());
                }
                else {
                    if (QueryUtils.isSqlType(tbl.rowDescriptor().type().valueClass()))
                        throw new SchemaOperationException("Cannot add column(s) because table was created " +
                            "with " + PARAM_WRAP_VALUE + "=false option.");

                    List<QueryField> cols = new ArrayList<>(cmd.columns().length);

                    boolean allFieldsNullable = true;

                    for (GridSqlColumn col : cmd.columns()) {
                        if (tbl.doesColumnExist(col.columnName())) {
                            if ((!cmd.ifNotExists() || cmd.columns().length != 1)) {
                                throw new SchemaOperationException(SchemaOperationException.CODE_COLUMN_EXISTS,
                                    col.columnName());
                            }
                            else {
                                cols = null;

                                break;
                            }
                        }

                        QueryField field = new QueryField(
                            col.columnName(),
                            getTypeClassName(col),
                            col.column().isNullable(),
                            col.defaultValue(),
                            convertH2ColumnPrecision(col.column().getType()),
                            convertH2ColumnScale(col.column().getType())
                        );

                        cols.add(field);

                        allFieldsNullable &= field.isNullable();
                    }

                    if (cols != null) {
                        assert tbl.rowDescriptor() != null;

                        if (!allFieldsNullable)
                            QueryUtils.checkNotNullAllowed(tbl.cacheInfo().config());

                        fut = ctx.query().dynamicColumnAdd(tbl.cacheName(), cmd.schemaName(),
                            tbl.rowDescriptor().type().tableName(), cols, cmd.ifTableExists(), cmd.ifNotExists());
                    }
                }
            }
            else if (cmdH2 instanceof GridSqlAlterTableDropColumn) {
                GridSqlAlterTableDropColumn cmd = (GridSqlAlterTableDropColumn)cmdH2;

                isDdlOnSchemaSupported(cmd.schemaName());

                GridH2Table tbl = schemaMgr.dataTable(cmd.schemaName(), cmd.tableName());

                if (tbl == null) {
                    if (!cmd.ifTableExists())
                        throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND,
                            cmd.tableName());
                }
                else {
                    assert tbl.rowDescriptor() != null;

                    if (tbl.cacheInfo().mvccEnabled())
                        throw new IgniteSQLException("Cannot drop column(s) with enabled MVCC. " +
                            "Operation is unsupported at the moment.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

                    if (QueryUtils.isSqlType(tbl.rowDescriptor().type().valueClass()))
                        throw new SchemaOperationException("Cannot drop column(s) because table was created " +
                            "with " + PARAM_WRAP_VALUE + "=false option.");

                    List<String> cols = new ArrayList<>(cmd.columns().length);

                    GridQueryTypeDescriptor type = tbl.rowDescriptor().type();

                    for (String colName : cmd.columns()) {
                        if (!tbl.doesColumnExist(colName)) {
                            if ((!cmd.ifExists() || cmd.columns().length != 1)) {
                                throw new SchemaOperationException(SchemaOperationException.CODE_COLUMN_NOT_FOUND,
                                    colName);
                            }
                            else {
                                cols = null;

                                break;
                            }
                        }

                        SchemaOperationException err = QueryUtils.validateDropColumn(type, colName);

                        if (err != null)
                            throw err;

                        cols.add(colName);
                    }

                    if (cols != null) {
                        fut = ctx.query().dynamicColumnRemove(tbl.cacheName(), cmd.schemaName(),
                            type.tableName(), cols, cmd.ifTableExists(), cmd.ifExists());
                    }
                }
            }
            else
                throw new IgniteSQLException("Unsupported DDL operation: " + sql,
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

            if (fut != null)
                fut.get();
        }
        catch (SchemaOperationException e) {
            U.error(null, "DDL operation failure", e);
            throw convert(e);
        }
        catch (IgniteSQLException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteSQLException(e.getMessage(), e);
        }
    }

    /**
     * Check if schema supports DDL statement.
     *
     * @param schemaName Schema name.
     */
    private static void isDdlOnSchemaSupported(String schemaName) {
        if (F.eq(QueryUtils.sysSchemaName(), schemaName))
            throw new IgniteSQLException("DDL statements are not supported on " + schemaName + " schema",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Check if table supports DDL statement.
     *
     * @param tbl Table.
     * @throws IgniteSQLException If failed.
     */
    private static void ensureDdlSupported(GridH2Table tbl) throws IgniteSQLException {
        if (tbl.cacheInfo().config().getCacheMode() == CacheMode.LOCAL)
            throw new IgniteSQLException("DDL statements are not supported on LOCAL caches",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Commits active transaction if exists.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void finishActiveTxIfNecessary() throws IgniteCheckedException {
        try (GridNearTxLocal tx = MvccUtils.tx(ctx)) {
            if (tx == null)
                return;

            if (!tx.isRollbackOnly())
                tx.commit();
            else
                tx.rollback();
        }
    }

    /**
     * @return {@link IgniteSQLException} with the message same as of {@code this}'s and
     */
    private IgniteSQLException convert(SchemaOperationException e) {
        int sqlCode;

        switch (e.code()) {
            case SchemaOperationException.CODE_CACHE_NOT_FOUND:
                sqlCode = IgniteQueryErrorCode.CACHE_NOT_FOUND;

                break;

            case SchemaOperationException.CODE_TABLE_NOT_FOUND:
                sqlCode = IgniteQueryErrorCode.TABLE_NOT_FOUND;

                break;

            case SchemaOperationException.CODE_TABLE_EXISTS:
                sqlCode = IgniteQueryErrorCode.TABLE_ALREADY_EXISTS;

                break;

            case SchemaOperationException.CODE_COLUMN_NOT_FOUND:
                sqlCode = IgniteQueryErrorCode.COLUMN_NOT_FOUND;

                break;

            case SchemaOperationException.CODE_COLUMN_EXISTS:
                sqlCode = IgniteQueryErrorCode.COLUMN_ALREADY_EXISTS;

                break;

            case SchemaOperationException.CODE_INDEX_NOT_FOUND:
                sqlCode = IgniteQueryErrorCode.INDEX_NOT_FOUND;

                break;

            case SchemaOperationException.CODE_INDEX_EXISTS:
                sqlCode = IgniteQueryErrorCode.INDEX_ALREADY_EXISTS;

                break;

            default:
                sqlCode = IgniteQueryErrorCode.UNKNOWN;
        }

        return new IgniteSQLException(e.getMessage(), sqlCode, e);
    }

    /**
     * Convert this statement to query entity and do Ignite specific sanity checks on the way.
     * @return Query entity mimicking this SQL statement.
     */
    private static QueryEntity toQueryEntity(GridKernalContext ctx, GridSqlCreateTable createTbl) {
        QueryEntityEx res = new QueryEntityEx();

        res.setTableName(createTbl.tableName());
        res.setImplicitPk(createTbl.implicitPk());

        Set<String> notNullFields = null;

        HashMap<String, Object> dfltValues = new HashMap<>();

        Map<String, Integer> precision = new HashMap<>();
        Map<String, Integer> scale = new HashMap<>();

        for (Map.Entry<String, GridSqlColumn> e : createTbl.columns().entrySet()) {
            GridSqlColumn gridCol = e.getValue();

            Column col = gridCol.column();

            res.addQueryField(e.getKey(), getTypeClassName(gridCol), null);

            if (!col.isNullable()) {
                if (notNullFields == null)
                    notNullFields = new HashSet<>();

                notNullFields.add(e.getKey());
            }

            Object dfltVal = gridCol.defaultValue();

            if (dfltVal != null)
                dfltValues.put(e.getKey(), dfltVal);

            int precisionVal = convertH2ColumnPrecision(col.getType());
            if (precisionVal != QueryField.UNDEFINED_PRECISION) {
                precision.put(e.getKey(), precisionVal);
            }

            int scaleVal = convertH2ColumnScale(col.getType());
            if (scaleVal != QueryField.UNDEFINED_SCALE) {
                scale.put(e.getKey(), scaleVal);
            }
        }

        if (!F.isEmpty(dfltValues))
            res.setDefaultFieldValues(dfltValues);

        if (!F.isEmpty(precision))
            res.setFieldsPrecision(precision);

        if (!F.isEmpty(scale))
            res.setFieldsScale(scale);

        String digest = createFieldsDigest(createTbl);
        String valTypeName = QueryUtils.createTableValueTypeName(createTbl.schemaName(), createTbl.tableName(), digest);

        String keyTypeName = QueryUtils.createTableKeyTypeName(valTypeName);

        if (!F.isEmpty(createTbl.keyTypeName()))
            keyTypeName = createTbl.keyTypeName();

        if (!F.isEmpty(createTbl.valueTypeName()))
            valTypeName = createTbl.valueTypeName();

        assert createTbl.wrapKey() != null;
        assert createTbl.wrapValue() != null;

        if (!createTbl.wrapKey() && !createTbl.implicitPk()) {
            GridSqlColumn pkCol = createTbl.columns().get(createTbl.primaryKeyColumns().iterator().next());

            keyTypeName = getTypeClassName(pkCol);

            res.setKeyFieldName(pkCol.columnName());
        }
        else if (createTbl.wrapKey()) {
            res.setKeyFields(createTbl.primaryKeyColumns());

            if (IgniteFeatures.allNodesSupports(ctx, F.view(ctx.discovery().allNodes(),
                IgniteDiscoverySpi.SRV_NODES), IgniteFeatures.SPECIFIED_SEQ_PK_KEYS)
            )
                res.setPreserveKeysOrder(true);
        }

        if (!createTbl.wrapValue()) {
            GridSqlColumn valCol = null;

            for (Map.Entry<String, GridSqlColumn> e : createTbl.columns().entrySet()) {
                if (!createTbl.primaryKeyColumns().contains(e.getKey())) {
                    valCol = e.getValue();

                    break;
                }
            }

            assert valCol != null;

            valTypeName = getTypeClassName(valCol);

            res.setValueFieldName(valCol.columnName());
        }

        res.setValueType(valTypeName);
        res.setKeyType(keyTypeName);

        if (!F.isEmpty(notNullFields))
            res.setNotNullFields(notNullFields);
        
        if (IgniteFeatures.allNodesSupports(ctx, F.view(ctx.discovery().allNodes(),
                IgniteDiscoverySpi.ALL_NODES), IgniteFeatures.FILLS_ABSENT_PKS_WITH_DEFAULTS)
        )
            res.fillAbsentPKsWithDefaults(true);

        if (Objects.nonNull(createTbl.primaryKeyInlineSize()))
            res.setPrimaryKeyInlineSize(createTbl.primaryKeyInlineSize());

        if (Objects.nonNull(createTbl.affinityKeyInlineSize()))
            res.setAffinityKeyInlineSize(createTbl.affinityKeyInlineSize());

        return res;
    }

    private static int convertH2ColumnPrecision(TypeInfo type) {
        if (type.getValueType() == Value.DECIMAL) {
            if (type.getPrecision() < H2Utils.DECIMAL_DEFAULT_PRECISION)
                return (int)type.getPrecision();
        }
        else if (type.getValueType() == Value.STRING ||
            type.getValueType() == Value.STRING_FIXED ||
            type.getValueType() == Value.STRING_IGNORECASE) {
            if (type.getPrecision() < H2Utils.STRING_DEFAULT_PRECISION)
                return (int)type.getPrecision();
        }

        return QueryField.UNDEFINED_PRECISION;
    }

    private static int convertH2ColumnScale(TypeInfo type) {
        if (type.getValueType() == Value.DECIMAL &&
            type.getScale() < H2Utils.DECIMAL_DEFAULT_SCALE)
            return type.getScale();

        return QueryField.UNDEFINED_SCALE;
    }

    /**
     * @param cmd Statement.
     * @return Whether {@code cmd} is a DDL statement we're able to handle.
     */
    public static boolean isCommand(Prepared cmd) {
        return cmd instanceof CreateIndex || cmd instanceof DropIndex || cmd instanceof CreateTable ||
            cmd instanceof DropTable || cmd instanceof AlterTableAlterColumn;
    }

    /**
     * @param cmd Statement.
     * @return Whether {@code cmd} is a no-op.
     */
    public static boolean isCommandNoOp(Prepared cmd) {
        return cmd instanceof NoOperation;
    }

    /**
     * Creates table digest as MD5 hash from sorted list of column names and types.
     *
     * @param tbl Create table command.
     * @return Digest from sorted list of column names and types.
     */
    private static String createFieldsDigest(GridSqlCreateTable tbl) {
        try {
            String concatedFields = concatFields(tbl);

           return U.calculateMD5(new ByteArrayInputStream(concatedFields.getBytes()));
        }
        catch (NoSuchAlgorithmException | IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param tbl Table.
     * @return Concated table fields and their types.
     */
    private static String concatFields(GridSqlCreateTable tbl) {
        List<String> fieldDigests = new ArrayList<>(tbl.columns().size());

        for (Map.Entry<String, GridSqlColumn> e : tbl.columns().entrySet()) {
            String colName = e.getKey();
            String colType = getTypeClassName(e.getValue());

            String fd = "[" + colName + ":" + colType + "]";

            fieldDigests.add(fd.toUpperCase());
        }

        Collections.sort(fieldDigests);

        return String.join(", ", fieldDigests);
    }

    /**
     * Helper function for obtaining type class name for H2.
     *
     * @param col Column.
     * @return Type class name.
     */
    private static String getTypeClassName(GridSqlColumn col) {
        int type = col.column().getType().getValueType();

        switch (type) {
            case Value.UUID :
                if (!handleUuidAsByte)
                    return UUID.class.getName();

            default:
                return DataType.getTypeClassName(type, false);
        }
    }

    /**
     * Process transactional command.
     * @param cmd Command.
     * @param params Parameters.
     * @throws IgniteCheckedException if failed.
     */
    private void processTxCommand(SqlCommand cmd, QueryParameters params)
        throws IgniteCheckedException {
        NestedTxMode nestedTxMode = params.nestedTxMode();

        GridNearTxLocal tx = tx(ctx);

        if (cmd instanceof SqlBeginTransactionCommand) {
            if (!mvccEnabled(ctx))
                throw new IgniteSQLException("MVCC must be enabled in order to start transaction.",
                    IgniteQueryErrorCode.MVCC_DISABLED);

            if (tx != null) {
                if (nestedTxMode == null)
                    nestedTxMode = NestedTxMode.DEFAULT;

                switch (nestedTxMode) {
                    case COMMIT:
                        doCommit(tx);

                        txStart(ctx, params.timeout());

                        break;

                    case IGNORE:
                        log.warning("Transaction has already been started, ignoring BEGIN command.");

                        break;

                    case ERROR:
                        throw new IgniteSQLException("Transaction has already been started.",
                            IgniteQueryErrorCode.TRANSACTION_EXISTS);

                    default:
                        throw new IgniteSQLException("Unexpected nested transaction handling mode: " +
                            nestedTxMode.name());
                }
            }
            else
                txStart(ctx, params.timeout());
        }
        else if (cmd instanceof SqlCommitTransactionCommand) {
            // Do nothing if there's no transaction.
            if (tx != null)
                doCommit(tx);
        }
        else {
            assert cmd instanceof SqlRollbackTransactionCommand;

            // Do nothing if there's no transaction.
            if (tx != null)
                doRollback(tx);
        }
    }

    /**
     * Commit and properly close transaction.
     * @param tx Transaction.
     * @throws IgniteCheckedException if failed.
     */
    private void doCommit(@NotNull GridNearTxLocal tx) throws IgniteCheckedException {
        try {
            tx.commit();
        }
        finally {
            closeTx(tx);
        }
    }

    /**
     * Rollback and properly close transaction.
     * @param tx Transaction.
     * @throws IgniteCheckedException if failed.
     */
    public void doRollback(@NotNull GridNearTxLocal tx) throws IgniteCheckedException {
        try {
            tx.rollback();
        }
        finally {
            closeTx(tx);
        }
    }

    /**
     * Properly close transaction.
     * @param tx Transaction.
     * @throws IgniteCheckedException if failed.
     */
    private void closeTx(@NotNull GridNearTxLocal tx) throws IgniteCheckedException {
        try {
            tx.close();
        }
        finally {
            ctx.cache().context().tm().resetContext();
        }
    }

    /**
     * Process SET STREAMING command.
     *
     * @param cmd Command.
     * @param cliCtx Client context.
     */
    private void processSetStreamingCommand(SqlSetStreamingCommand cmd,
        @Nullable SqlClientContext cliCtx) {
        if (cliCtx == null)
            throw new IgniteSQLException("SET STREAMING command can only be executed from JDBC or ODBC driver.");

        if (cmd.isTurnOn())
            cliCtx.enableStreaming(
                cmd.allowOverwrite(),
                cmd.flushFrequency(),
                cmd.perNodeBufferSize(),
                cmd.perNodeParallelOperations(),
                cmd.isOrdered()
            );
        else
            cliCtx.disableStreaming();
    }
}

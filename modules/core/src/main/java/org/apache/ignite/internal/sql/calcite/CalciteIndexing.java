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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableInterpreter;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
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
import org.apache.ignite.internal.sql.calcite.physical.Filter;
import org.apache.ignite.internal.sql.calcite.physical.PhysicalOperator;
import org.apache.ignite.internal.sql.calcite.physical.Project;
import org.apache.ignite.internal.sql.calcite.physical.TableScan;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.TimeBag;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.tools.Programs.ofRules;

/**
 * TODO: Add class description.
 */
public class CalciteIndexing implements GridQueryIndexing {

    private GridKernalContext ctx;

    private final SchemaPlus rootSchema = Frameworks.createRootSchema(true);

    private static final List<RelTraitDef> TRAITS = Collections
        .unmodifiableList(Arrays.asList(ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE)); // TODO ??????????

    private static final SqlParser.Config CALCITE_PARSER_CONFIG
        = SqlParser.configBuilder(SqlParser.Config.DEFAULT)
        .setCaseSensitive(false)
        //.setConformance(SqlConformanceEnum.MYSQL_5)
        //.setQuoting(Quoting.BACK_TICK)
        .build();

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
                schema.add(cand.descriptor().tableName(), new IgniteTable(cand, cacheName));
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
        try {
            System.out.println("CalciteIndexing.querySqlFields");

            TimeBag timeBag = new TimeBag();

            Planner planner = getPlanner(schemaName);

            timeBag.finishGlobalStage("Planner created");

            // 1. Parsing
            SqlNode sqlAst = planner.parse(qry.getSql());

            timeBag.finishGlobalStage("Query parsed");

            // 2. Validating
            sqlAst = planner.validate(sqlAst);

            timeBag.finishGlobalStage("Query validated");

            // 3. Converting AST to RelTree
            RelNode logicalPlan = planner.rel(sqlAst).project();

            System.out.println("Initial logical plan:\n" + RelOptUtil.toString(logicalPlan));

            timeBag.finishGlobalStage("Logical planning finished");

            RelOptCluster cluster = logicalPlan.getCluster();

            final RelOptPlanner optPlanner = cluster.getPlanner();

            RelTraitSet desiredTraits
                = cluster.traitSet()
                .replace(EnumerableConvention.INSTANCE);

            final RelCollation collation
                = logicalPlan instanceof Sort
                ? ((Sort)logicalPlan).collation
                : null;

            if (collation != null) {
                desiredTraits = desiredTraits.replace(collation);
            }

            final RelNode newRoot = optPlanner.changeTraits(logicalPlan, desiredTraits);
            optPlanner.setRoot(newRoot);

            timeBag.finishGlobalStage("Planner prepared");

            RelNode bestPlan = optPlanner.findBestExp();

            timeBag.finishGlobalStage("Best plan found");

            System.out.println("Optimal plan:\n" + RelOptUtil.toString(bestPlan));
            System.out.println("Planning timings=" + timeBag.stagesTimings());

            PhysicalOperator physicalOperator = convertToPhysical(bestPlan);

            return Collections.singletonList(new QueryCursorImpl<>(physicalOperator));

        }
        catch (SqlParseException | ValidationException | RelConversionException e) {
            throw new IgniteException("Parsing error.", e);
        }
    }



    @NotNull private Planner getPlanner(String schemaName) {
        SchemaPlus schema = rootSchema.getSubSchema(schemaName);

        if (schema == null)
            throw new IgniteException("No schema: " + schemaName);

        final FrameworkConfig config = Frameworks.newConfigBuilder()
            .parserConfig(CALCITE_PARSER_CONFIG)
            .defaultSchema(schema)
            .traitDefs(TRAITS)
            .programs(ofRules(CalciteUtils.RULE_SET)) // TODO add rules
            .build();

        return Frameworks.getPlanner(config);
    }

    private PhysicalOperator convertToPhysical(RelNode plan) {
        if (plan instanceof Bindables.BindableTableScan)
            return convertToTableScan((Bindables.BindableTableScan)plan);
        if (plan instanceof EnumerableTableScan)
            return convertToTableScan((EnumerableTableScan)plan); // TODO how to control which scan is used?
        else if (plan instanceof EnumerableInterpreter)
            return convertToPhysical(plan.getInput(0)); // EnumerableInterpreter is just an adapter between conventions.
        else if (plan instanceof EnumerableProject)
            return convertToProject((EnumerableProject)plan);
        else if (plan instanceof EnumerableFilter)
            return convertToFilter((EnumerableFilter)plan);

        throw new IgniteException("Operation is not supported yet: " + plan);
    }

    private PhysicalOperator convertToFilter(EnumerableFilter plan) {
        PhysicalOperator rowsSrc = convertToPhysical(plan.getInput());

        return new Filter(rowsSrc, plan.getCondition());  // TODO: implement.
    }

    private Project convertToProject(EnumerableProject plan) {
        PhysicalOperator rowsSrc = convertToPhysical(plan.getInput());

        List<RexNode> projects = plan.getProjects();

        return new Project(rowsSrc, projects);
    }

    @NotNull private TableScan convertToTableScan(EnumerableTableScan plan) {
        List<String> tblName = plan.getTable().getQualifiedName(); // Schema + tblName

        IgniteTable tbl = (IgniteTable)rootSchema.getSubSchema(tblName.get(0)).getTable(tblName.get(1));

        return new TableScan(tbl, ctx.cache().cache(tbl.cacheName()));
    }

    @NotNull private TableScan convertToTableScan(Bindables.BindableTableScan plan) {
        List<String> tblName = plan.getTable().getQualifiedName(); // Schema + tblName

        IgniteTable tbl = (IgniteTable)rootSchema.getSubSchema(tblName.get(0)).getTable(tblName.get(1));

        return new TableScan(tbl, ctx.cache().cache(tbl.cacheName()));
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

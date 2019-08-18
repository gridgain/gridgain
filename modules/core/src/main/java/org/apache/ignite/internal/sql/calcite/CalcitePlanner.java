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
package org.apache.ignite.internal.sql.calcite;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.AggregateStarTableRule;
import org.apache.calcite.rel.rules.CalcRemoveRule;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.ExchangeRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.IntersectToDistinctRule;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.rules.UnionPullUpConstantsRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.sql.calcite.iterators.PhysicalOperator;

/**
 * TODO: REUSE AS {@link PlannerImpl}
 */
public class CalcitePlanner {

    private static final List<RelTraitDef> TRAIT_DEFS = Collections.unmodifiableList(Arrays.asList(
        ConventionTraitDef.INSTANCE,
        RelCollationTraitDef.INSTANCE,
        RelDistributionTraitDef.INSTANCE)
    );

    private static final SqlParser.Config CALCITE_PARSER_CONFIG
        = SqlParser.configBuilder(SqlParser.Config.DEFAULT)
        .setCaseSensitive(false)
        //.setConformance(SqlConformanceEnum.MYSQL_5)
        //.setQuoting(Quoting.BACK_TICK)
        .build();

    private static final Collection<RelOptRule> HEP_RULES = Arrays.asList(
        ProjectFilterTransposeRule.INSTANCE
    );

    private static final Collection<RelOptRule> CBO_RULES = Arrays.asList(
        // Abstract relational rules
        FilterJoinRule.FILTER_ON_JOIN,
        FilterJoinRule.JOIN,
        AbstractConverter.ExpandConversionRule.INSTANCE,
        JoinCommuteRule.INSTANCE,
        SemiJoinRule.PROJECT,
        SemiJoinRule.JOIN,
        AggregateRemoveRule.INSTANCE,
        UnionToDistinctRule.INSTANCE,
        ProjectRemoveRule.INSTANCE,
        AggregateJoinTransposeRule.INSTANCE,
        AggregateMergeRule.INSTANCE,
        AggregateProjectMergeRule.INSTANCE,
        CalcRemoveRule.INSTANCE,
        SortRemoveRule.INSTANCE,

        // Basic rules
        AggregateStarTableRule.INSTANCE,
        AggregateStarTableRule.INSTANCE2,
        TableScanRule.INSTANCE,
        CalciteSystemProperty.COMMUTE.value()
            ? JoinAssociateRule.INSTANCE
            : ProjectMergeRule.INSTANCE,
        FilterTableScanRule.INSTANCE,
        ProjectFilterTransposeRule.INSTANCE,
        FilterProjectTransposeRule.INSTANCE,
        FilterJoinRule.FILTER_ON_JOIN,
        JoinPushExpressionsRule.INSTANCE,
        AggregateExpandDistinctAggregatesRule.INSTANCE,
        AggregateReduceFunctionsRule.INSTANCE,
        FilterAggregateTransposeRule.INSTANCE,
        ProjectWindowTransposeRule.INSTANCE,
        JoinCommuteRule.INSTANCE,
        JoinPushThroughJoinRule.RIGHT,
        JoinPushThroughJoinRule.LEFT,
        SortProjectTransposeRule.INSTANCE,
        SortJoinTransposeRule.INSTANCE,
        SortRemoveConstantKeysRule.INSTANCE,
        SortUnionTransposeRule.INSTANCE,
        ExchangeRemoveConstantKeysRule.EXCHANGE_INSTANCE,
        ExchangeRemoveConstantKeysRule.SORT_EXCHANGE_INSTANCE,

        // Abstract rules

        AggregateProjectPullUpConstantsRule.INSTANCE2,
        UnionPullUpConstantsRule.INSTANCE,
        PruneEmptyRules.UNION_INSTANCE,
        PruneEmptyRules.INTERSECT_INSTANCE,
        PruneEmptyRules.MINUS_INSTANCE,
        PruneEmptyRules.PROJECT_INSTANCE,
        PruneEmptyRules.FILTER_INSTANCE,
        PruneEmptyRules.SORT_INSTANCE,
        PruneEmptyRules.AGGREGATE_INSTANCE,
        PruneEmptyRules.JOIN_LEFT_INSTANCE,
        PruneEmptyRules.JOIN_RIGHT_INSTANCE,
        PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
        UnionMergeRule.INSTANCE,
        UnionMergeRule.INTERSECT_INSTANCE,
        UnionMergeRule.MINUS_INSTANCE,
        ProjectToWindowRule.PROJECT,
        FilterMergeRule.INSTANCE,
        DateRangeRules.FILTER_INSTANCE,
        IntersectToDistinctRule.INSTANCE,

        // Ignite convention converters
        CalciteUtils.FILTER_RULE,
        CalciteUtils.JOIN_RULE,
        CalciteUtils.PROJECT_RULE,
        CalciteUtils.TABLE_SCAN_RULE,
        CalciteUtils.EXCHANGE_RULE
    );

    private final SchemaPlus rootSchema;
    private final GridKernalContext ctx;
    private final JavaTypeFactory typeFactory;
    private final CalciteConnectionConfig connCfg;

    private final CalciteCatalogReader catalogReader;
    private final SqlValidator validator;
    private final HepPlanner hepPlanner;
    private final VolcanoPlanner cboPlanner;
    private final SqlToRelConverter sqlToRelConverter;

    public CalcitePlanner(SchemaPlus schema, SchemaPlus rootSchema, GridKernalContext ctx) {
        this.rootSchema = rootSchema;
        this.ctx = ctx;
        typeFactory = getTypeFactory();
        connCfg = getConnectionConfig();
        catalogReader = getCatalogReader(schema, typeFactory, connCfg);
        validator = getValidator(typeFactory, catalogReader, connCfg);
        hepPlanner = getHepPlanner();
        cboPlanner = getCboPlanner(connCfg);
        sqlToRelConverter = prepareSqlToRelConverter(typeFactory, catalogReader, validator, cboPlanner);

    }

    public PhysicalOperator createPlan(String sql) {
        SqlNode sqlAst = parse(sql);

        SqlNode validatedSqlAst = validate(sqlAst);

        RelNode logicalPlan = convertToRel(validatedSqlAst);
        System.out.println("Initial logical plan:\n" + RelOptUtil.toString(logicalPlan));

        RelNode rewrittenPlan = rewritePlan(logicalPlan);
        System.out.println("Rewritten logical plan:\n" + RelOptUtil.toString(rewrittenPlan));

        RelNode optimalPlan = optimizePlan(rewrittenPlan);
        System.out.println("Optimal plan:\n" + RelOptUtil.toString(optimalPlan, SqlExplainLevel.ALL_ATTRIBUTES));

        // TODO replace with a visitor
        PhysicalOperator physicalOperator = null; //convertToPhysical(optimalPlan);

        return physicalOperator;
    }

    private RelNode rewritePlan(RelNode plan) {

        return plan;
        // TODo decorrelation, see PlannerImpl.rel and Drill
//        hepPlanner.setRoot(plan);
//
//        return hepPlanner.findBestExp();
    }

    private RelNode optimizePlan(RelNode plan) {
        //cboPlanner.setNoneConventionHasInfiniteCost(false);EnumerableConvention.INSTANCE)
        RelTraitSet desiredTraits
            = plan.getCluster().traitSet()
            .replace(RelDistributions.SINGLETON)
            .replace(IgniteConvention.INSTANCE);

        final RelCollation collation // TODO collation
            = plan instanceof Sort
            ? ((Sort)plan).collation
            : null;

        if (collation != null)
            desiredTraits = desiredTraits.replace(collation);

        RelNode newRoot = cboPlanner.changeTraits(plan, desiredTraits);

        cboPlanner.setRoot(newRoot);

        cboPlanner.setNoneConventionHasInfiniteCost(true);

        return cboPlanner.findBestExp();
    }

    private SqlNode validate(SqlNode sqlAst) {
        return validator.validate(sqlAst);
    }

    private SqlNode parse(String sql) {
        try {
            SqlParser parser = SqlParser.create(sql, CALCITE_PARSER_CONFIG);

            return parser.parseStmt();
        }
        catch (SqlParseException e) {
            throw new IgniteException(e);
        }
    }

    private RelNode convertToRel(SqlNode node) {
        RelRoot root = sqlToRelConverter.convertQuery(node, false, true);

        return root.rel;
    }

    private HepPlanner getHepPlanner() {
        HepProgramBuilder hepBuilder = new HepProgramBuilder();

        hepBuilder.addRuleCollection(HEP_RULES);

        HepPlanner hepPlanner = new HepPlanner(
            hepBuilder.build()
        );

        return hepPlanner;
    }

    private VolcanoPlanner getCboPlanner(CalciteConnectionConfig config) {
        VolcanoPlanner planner = new VolcanoPlanner(null, Contexts.of(config));

        for (RelTraitDef def : TRAIT_DEFS)
            planner.addRelTraitDef(def);

        for (RelOptRule rule : CBO_RULES)
            planner.addRule(rule);

        return planner;
    }

    private SqlValidator getValidator(JavaTypeFactory typeFactory, CalciteCatalogReader catalogReader,
        CalciteConnectionConfig connCfg) {
        // TODO add operators to SqlStdOperatorTable
        return new IgniteValidator(SqlStdOperatorTable.instance(), catalogReader, typeFactory,
            connCfg.conformance());
    }

    private JavaTypeFactoryImpl getTypeFactory() {
        return new JavaTypeFactoryImpl();
    }

    private static CalciteConnectionConfigImpl getConnectionConfig() {
        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), String.valueOf(false));
        return new CalciteConnectionConfigImpl(props);
    }

    private CalciteCatalogReader getCatalogReader(SchemaPlus schema, JavaTypeFactory typeFactory,
        CalciteConnectionConfig connCfg) {
        final SchemaPlus rootSchema = rootSchema(schema);

        return new CalciteCatalogReader(
            CalciteSchema.from(rootSchema),
            CalciteSchema.from(schema).path(null),
            typeFactory, connCfg);
    }

    private static SchemaPlus rootSchema(SchemaPlus schema) {
        while (true) {
            if (schema.getParentSchema() == null)
                return schema;

            schema = schema.getParentSchema();
        }
    }

    private SqlToRelConverter prepareSqlToRelConverter(
        JavaTypeFactory typeFactory,
        Prepare.CatalogReader catalogReader,
        SqlValidator validator,
        VolcanoPlanner planner
    ) {
        SqlToRelConverter.ConfigBuilder sqlToRelConfigBuilder = SqlToRelConverter.configBuilder()
            .withTrimUnusedFields(true)
            .withExpand(false)
            .withExplain(false)
            .withConvertTableAccess(false);

        return new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            RelOptCluster.create(planner, new RexBuilder(typeFactory)),
            StandardConvertletTable.INSTANCE,
            sqlToRelConfigBuilder.build()
        );
    }

//    private PhysicalOperator convertToPhysical(RelNode plan) {
//        if (plan instanceof TableScanRel)
//            return convertToTableScan((TableScanRel)plan);
//        else if (plan instanceof ProjectRel)
//            return convertToProject((ProjectRel)plan);
//        else if (plan instanceof FilterRel)
//            return convertToFilter((FilterRel)plan);
//        else if (plan instanceof JoinNestedLoopsRel)
//            return convertToHashJoin((JoinNestedLoopsRel)plan);
//
//        throw new IgniteException("Operation is not supported yet: " + plan);
//    }
//
//    private PhysicalOperator convertToHashJoin(
//        JoinNestedLoopsRel plan) { // TODO why do we have HashJoin even for non-equi-joins?
//        PhysicalOperator leftSrc = convertToPhysical(plan.getInput(0));
//        PhysicalOperator rightSrc = convertToPhysical(plan.getInput(1));
//
//        ImmutableIntList leftJoinKeys = plan.getLeftKeys();
//        ImmutableIntList rightJoinKeys = plan.getRightKeys();
//
//        RexNode joinCond = plan.getCondition();
//
//        JoinRelType joinType = plan.getJoinType();
//
//        return new NestedLoopsJoinOp(leftSrc, rightSrc, leftJoinKeys, rightJoinKeys, joinCond, joinType);
//    }
//
//    private PhysicalOperator convertToFilter(FilterRel plan) {
//        PhysicalOperator rowsSrc = convertToPhysical(plan.getInput());
//
//        return new FilterOp(rowsSrc, plan.getCondition());
//    }
//
//    private ProjectOp convertToProject(ProjectRel plan) {
//        PhysicalOperator rowsSrc = convertToPhysical(plan.getInput());
//
//        List<RexNode> projects = plan.getProjects();
//
//        return new ProjectOp(rowsSrc, projects);
//    }
//
//    @NotNull private TableScanOp convertToTableScan(TableScanRel plan) {
//        List<String> tblName = plan.getTable().getQualifiedName(); // Schema + tblName
//
//        IgniteTable tbl = (IgniteTable)rootSchema.getSubSchema(tblName.get(0)).getTable(tblName.get(1));
//
//        return new TableScanOp(tbl, ctx.cache().cache(tbl.cacheName()));
//    }
}

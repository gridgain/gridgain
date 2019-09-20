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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import com.google.common.collect.ImmutableList;
import java.io.Reader;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;

/**
 *
 */
public class PlannerImpl implements Planner, RelOptTable.ViewExpander {
    private final SqlOperatorTable operatorTable;
    private final ImmutableList<Program> programs;
    private final FrameworkConfig frameworkConfig;
    private final Context context;
    private final CalciteConnectionConfig connectionConfig;
    private final ImmutableList<RelTraitDef> traitDefs;
    private final SqlParser.Config parserConfig;
    private final SqlToRelConverter.Config sqlToRelConverterConfig;
    private final SqlRexConvertletTable convertletTable;
    private final RexExecutor executor;
    private final SchemaPlus defaultSchema;
    private final JavaTypeFactory typeFactory;

    private boolean open;

    private RelOptPlanner planner;
    private SqlValidator validator;

    /**
     * @param config Framework config.
     */
    public PlannerImpl(FrameworkConfig config) {
        frameworkConfig = config;
        defaultSchema = config.getDefaultSchema();
        operatorTable = config.getOperatorTable();
        programs = config.getPrograms();
        parserConfig = config.getParserConfig();
        sqlToRelConverterConfig = config.getSqlToRelConverterConfig();
        traitDefs = config.getTraitDefs();
        convertletTable = config.getConvertletTable();
        executor = config.getExecutor();
        context = config.getContext();
        connectionConfig = connConfig();

        RelDataTypeSystem typeSystem = connectionConfig
            .typeSystem(RelDataTypeSystem.class, RelDataTypeSystem.DEFAULT);

        typeFactory = new JavaTypeFactoryImpl(typeSystem);
    }

    private CalciteConnectionConfig connConfig() {
        CalciteConnectionConfig unwrapped = context.unwrap(CalciteConnectionConfig.class);
        if (unwrapped != null)
            return unwrapped;

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(parserConfig.caseSensitive()));
        properties.setProperty(CalciteConnectionProperty.CONFORMANCE.camelName(),
            String.valueOf(frameworkConfig.getParserConfig().conformance()));
        return new CalciteConnectionConfigImpl(properties);
    }

    /** {@inheritDoc} */
    @Override public RelTraitSet getEmptyTraitSet() {
        return planner.emptyTraitSet();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        reset();
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        planner = null;
        validator = null;

        open = false;
    }

    private void ready() {
        if (!open) {
            planner = new VolcanoPlanner(frameworkConfig.getCostFactory(), context);
            planner.setExecutor(executor);

            validator = new IgniteSqlValidator(operatorTable, createCatalogReader(), typeFactory, conformance());
            validator.setIdentifierExpansion(true);

            for (RelTraitDef def : traitDefs) {
                planner.addRelTraitDef(def);
            }

            open = true;
        }
    }

    /** {@inheritDoc} */
    @Override public SqlNode parse(Reader reader) throws SqlParseException {
        return SqlParser.create(reader, parserConfig).parseStmt();
    }

    /** {@inheritDoc} */
    @Override public SqlNode validate(SqlNode sqlNode) throws ValidationException {
        ready();

        try {
            return validator.validate(sqlNode);
        }
        catch (RuntimeException e) {
            throw new ValidationException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode) throws ValidationException {
        ready();

        SqlNode validatedNode = validate(sqlNode);
        RelDataType type = validator.getValidatedNodeType(validatedNode);
        return Pair.of(validatedNode, type);
    }

    /** {@inheritDoc} */
    @Override public RelNode convert(SqlNode sql) {
        return rel(sql).rel;
    }

    /** {@inheritDoc} */
    @Override public RelRoot rel(SqlNode sql) {
        ready();

        RexBuilder rexBuilder = createRexBuilder();
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
            .withConfig(sqlToRelConverterConfig)
            .withTrimUnusedFields(false)
            .withConvertTableAccess(false)
            .build();
        SqlToRelConverter sqlToRelConverter =
            new SqlToRelConverter(this, validator, createCatalogReader(), cluster, convertletTable, config);
        RelRoot root = sqlToRelConverter.convertQuery(sql, false, true);
        root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
        RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
        root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
        return root;
    }

    /** {@inheritDoc} */
    @Override public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        ready();

        SqlParser parser = SqlParser.create(queryString, parserConfig);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        }
        catch (SqlParseException e) {
            throw new RuntimeException("parse failed", e);
        }

        SqlConformance conformance = conformance();
        CalciteCatalogReader catalogReader =
            createCatalogReader().withSchemaPath(schemaPath);
        SqlValidator validator = new IgniteSqlValidator(operatorTable, catalogReader, typeFactory, conformance);
        validator.setIdentifierExpansion(true);

        RexBuilder rexBuilder = createRexBuilder();
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        SqlToRelConverter.Config config = SqlToRelConverter
            .configBuilder()
            .withConfig(sqlToRelConverterConfig)
            .withTrimUnusedFields(false)
            .withConvertTableAccess(false)
            .build();
        SqlToRelConverter sqlToRelConverter =
            new SqlToRelConverter(this, validator,
                catalogReader, cluster, convertletTable, config);

        RelRoot root = sqlToRelConverter.convertQuery(sqlNode, true, false);
        RelRoot root2 = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
        RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
        return root2.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
    }

    /** {@inheritDoc} */
    @Override public RelNode transform(int ruleSetIndex, RelTraitSet requiredOutputTraits, RelNode rel) {
        ready();

        rel.getCluster()
            .setMetadataProvider(new CachingRelMetadataProvider(
                rel.getCluster().getMetadataProvider(), rel.getCluster().getPlanner()));
        Program program = programs.get(ruleSetIndex);
        return program.run(planner, rel, requiredOutputTraits, ImmutableList.of(), ImmutableList.of());
    }

    /** {@inheritDoc} */
    @Override public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }

    private SqlConformance conformance() {
        return connectionConfig.conformance();
    }

    private RexBuilder createRexBuilder() {
        return new RexBuilder(typeFactory);
    }

    private CalciteCatalogReader createCatalogReader() {
        SchemaPlus rootSchema = rootSchema(defaultSchema);

        return new CalciteCatalogReader(
            CalciteSchema.from(rootSchema),
            CalciteSchema.from(defaultSchema).path(null),
            typeFactory, connectionConfig);
    }

    private static SchemaPlus rootSchema(SchemaPlus schema) {
        for (; ; ) {
            if (schema.getParentSchema() == null) {
                return schema;
            }
            schema = schema.getParentSchema();
        }
    }
}

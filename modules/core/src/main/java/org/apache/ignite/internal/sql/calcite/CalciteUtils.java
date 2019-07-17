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

import java.util.Arrays;
import java.util.Collection;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateStarTableRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.jdbc2.JdbcUtils;

/**
 * TODO: Add class description.
 */
public class CalciteUtils {

    public static SqlTypeName classNameToSqlType(String clsName) {
        int jdbcType = JdbcUtils.type(clsName);

        return SqlTypeName.getNameForJdbcType(jdbcType);
    }

    public static final Collection<RelOptRule> RULE_SET =
        Arrays.asList(
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
            // The following line is to be removed before 1.21;
            // when it is removed, also remove SuppressWarnings directive, above
            EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
            EnumerableRules.ENUMERABLE_CORRELATE_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_LIMIT_RULE,
            EnumerableRules.ENUMERABLE_UNION_RULE,
            EnumerableRules.ENUMERABLE_INTERSECT_RULE,
            EnumerableRules.ENUMERABLE_MINUS_RULE,
            EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
            EnumerableRules.ENUMERABLE_VALUES_RULE,
            EnumerableRules.ENUMERABLE_WINDOW_RULE,
            SemiJoinRule.PROJECT,
            SemiJoinRule.JOIN,
            TableScanRule.INSTANCE,
            CalciteSystemProperty.COMMUTE.value()
                ? JoinAssociateRule.INSTANCE
                : ProjectMergeRule.INSTANCE,
            AggregateStarTableRule.INSTANCE,
            AggregateStarTableRule.INSTANCE2,
            FilterTableScanRule.INSTANCE,
            FilterProjectTransposeRule.INSTANCE,
            FilterJoinRule.FILTER_ON_JOIN,
            AggregateExpandDistinctAggregatesRule.INSTANCE,
            AggregateReduceFunctionsRule.INSTANCE,
            FilterAggregateTransposeRule.INSTANCE,
            JoinCommuteRule.INSTANCE,
            JoinPushThroughJoinRule.RIGHT,
            JoinPushThroughJoinRule.LEFT,
            SortProjectTransposeRule.INSTANCE);

}

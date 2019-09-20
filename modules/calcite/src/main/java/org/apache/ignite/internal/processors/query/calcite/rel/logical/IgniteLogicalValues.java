/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.rel.logical;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;

public class IgniteLogicalValues extends Values implements IgniteRel {
  public IgniteLogicalValues(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType rowType,
      ImmutableList<ImmutableList<RexLiteral>> tuples) {
    super(cluster, rowType, tuples, traitSet);
  }

  public IgniteLogicalValues(RelInput input) {
    super(input);
  }

  public static IgniteLogicalValues create(RelOptCluster cluster,
      final RelDataType rowType,
      final ImmutableList<ImmutableList<RexLiteral>> tuples) {
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet = cluster.traitSetOf(IgniteRel.LOGICAL_CONVENTION)
        .replaceIfs(RelCollationTraitDef.INSTANCE,
            () -> RelMdCollation.values(mq, rowType, tuples));
    return new IgniteLogicalValues(cluster, traitSet, rowType, tuples);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new IgniteLogicalValues(getCluster(), traitSet, rowType, tuples);
  }

  public static IgniteLogicalValues createEmpty(RelOptCluster cluster,
      RelDataType rowType) {
    return create(cluster, rowType,
        ImmutableList.of());
  }

  public static IgniteLogicalValues createOneRow(RelOptCluster cluster) {
    final RelDataType rowType =
        cluster.getTypeFactory().builder()
            .add("ZERO", SqlTypeName.INTEGER).nullable(false)
            .build();
    final ImmutableList<ImmutableList<RexLiteral>> tuples =
        ImmutableList.of(
            ImmutableList.of(
                cluster.getRexBuilder().makeExactLiteral(BigDecimal.ZERO,
                    rowType.getFieldList().get(0).getType())));
    return create(cluster, rowType, tuples);
  }
}

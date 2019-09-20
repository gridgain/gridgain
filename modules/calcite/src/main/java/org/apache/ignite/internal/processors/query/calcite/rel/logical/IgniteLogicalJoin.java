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
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;

public final class IgniteLogicalJoin extends Join implements IgniteRel {
  private final boolean semiJoinDone;

  private final ImmutableList<RelDataTypeField> systemFieldList;

  public IgniteLogicalJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType,
      boolean semiJoinDone,
      ImmutableList<RelDataTypeField> systemFieldList) {
    super(cluster, traitSet, left, right, condition, variablesSet, joinType);
    this.semiJoinDone = semiJoinDone;
    this.systemFieldList = Objects.requireNonNull(systemFieldList);
  }

  public IgniteLogicalJoin(RelInput input) {
    this(input.getCluster(), input.getCluster().traitSetOf(IgniteRel.LOGICAL_CONVENTION),
        input.getInputs().get(0), input.getInputs().get(1),
        input.getExpression("condition"), ImmutableSet.of(),
        input.getEnum("joinType", JoinRelType.class), false,
        ImmutableList.of());
  }

  public static IgniteLogicalJoin create(RelNode left, RelNode right,
      RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType,
      boolean semiJoinDone, ImmutableList<RelDataTypeField> systemFieldList) {
    final RelOptCluster cluster = left.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(IgniteRel.LOGICAL_CONVENTION);
    return new IgniteLogicalJoin(cluster, traitSet, left, right, condition,
        variablesSet, joinType, semiJoinDone, systemFieldList);
  }

  public static IgniteLogicalJoin create(RelNode left, RelNode right,
      RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
    return create(left, right, condition, variablesSet, joinType, false,
        ImmutableList.of());
  }

  @Override public IgniteLogicalJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
      RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new IgniteLogicalJoin(getCluster(),
        getCluster().traitSetOf(IgniteRel.LOGICAL_CONVENTION), left, right, conditionExpr,
        variablesSet, joinType, semiJoinDone, systemFieldList);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    // Don't ever print semiJoinDone=false. This way, we
    // don't clutter things up in optimizers that don't use semi-joins.
    return super.explainTerms(pw)
        .itemIf("semiJoinDone", semiJoinDone, semiJoinDone);
  }

  @Override public boolean isSemiJoinDone() {
    return semiJoinDone;
  }

  @Override public List<RelDataTypeField> getSystemFieldList() {
    return systemFieldList;
  }
}
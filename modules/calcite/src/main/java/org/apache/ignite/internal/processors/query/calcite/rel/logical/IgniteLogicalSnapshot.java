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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;

public class IgniteLogicalSnapshot extends Snapshot implements IgniteRel {
  public IgniteLogicalSnapshot(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RexNode period) {
    super(cluster, traitSet, input, period);
  }

  @Override public Snapshot copy(RelTraitSet traitSet, RelNode input,
      RexNode period) {
    return new IgniteLogicalSnapshot(getCluster(), traitSet, input, period);
  }

  public static IgniteLogicalSnapshot create(RelNode input, RexNode period) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet = cluster.traitSet()
        .replace(IgniteRel.LOGICAL_CONVENTION)
        .replaceIfs(RelCollationTraitDef.INSTANCE,
            () -> RelMdCollation.snapshot(mq, input))
        .replaceIf(RelDistributionTraitDef.INSTANCE,
            () -> RelMdDistribution.snapshot(mq, input));
    return new IgniteLogicalSnapshot(cluster, traitSet, input, period);
  }
}

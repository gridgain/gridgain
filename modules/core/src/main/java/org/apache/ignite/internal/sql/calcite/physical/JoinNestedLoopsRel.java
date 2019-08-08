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
package org.apache.ignite.internal.sql.calcite.physical;

import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.EquiJoin;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

/**
 * TODO: Add class description.
 */
public class JoinNestedLoopsRel extends EquiJoin implements IgniteRel {

    protected JoinNestedLoopsRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode left,
        RelNode right, RexNode condition, Set<CorrelationId> variablesSet,
        JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType,
        boolean semiJoinDone) {
        return new JoinNestedLoopsRel(getCluster(), traitSet, left, right, condition, getVariablesSet(), joinType);
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("dist", getTraitSet().getTrait(IgniteDistributionTraitDef.INSTANCE));
    }
}

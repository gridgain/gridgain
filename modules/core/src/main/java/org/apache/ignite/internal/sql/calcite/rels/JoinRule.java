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
package org.apache.ignite.internal.sql.calcite.rels;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.calcite.IgniteConvention;

/**
 * TODO: Add class description.
 */
public class JoinRule extends ConverterRule {

    public <R extends RelNode> JoinRule() {
        super(LogicalJoin.class,
            Convention.NONE,
            IgniteConvention.INSTANCE,
            "IgniteJoinRule");
    }

    @Override public RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;

        final JoinInfo info = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition()); // TODO take condition from info? See EnumerableJoinRule

        final RelNode left = convertInput(join.getInputs().get(0), info.leftKeys);;
        final RelNode right = convertInput(join.getInputs().get(1), info.rightKeys);

        //System.out.println("info.leftKeys=" + info.leftKeys + ", info.rightKeys=" +  info.rightKeys);

        return new JoinNestedLoopsRel(join.getCluster(),
            rel.getTraitSet().replace(IgniteConvention.INSTANCE).replace(RelDistributions.hash(info.leftKeys)), // TODO distribution?
            left,
            right,
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType());
    }

    private RelNode convertInput(RelNode input, ImmutableIntList joinKeys) {
        //if (!(input.getConvention() instanceof IgniteConvention)) {
            input = convert(input,
                    input.getTraitSet().replace(IgniteConvention.INSTANCE).replace(RelDistributions.hash(joinKeys)));
        //}

        return input;
    }
}

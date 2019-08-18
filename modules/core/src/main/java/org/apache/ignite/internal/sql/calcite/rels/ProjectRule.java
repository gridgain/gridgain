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

import java.util.function.Predicate;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.ignite.internal.sql.calcite.IgniteConvention;

/**
 * TODO: Add class description.
 */
public class ProjectRule extends ConverterRule {
    public ProjectRule() {
        super(LogicalProject.class,
            (Predicate<LogicalProject>) RelOptUtil::containsMultisetOrWindowedAgg, // TODO why should we filter it?
            Convention.NONE, IgniteConvention.INSTANCE,
            RelFactories.LOGICAL_BUILDER, "IgniteProjectRule");
    }

    @Override public RelNode convert(RelNode rel) {
        LogicalProject proj = (LogicalProject)rel;

        return new ProjectRel(rel.getCluster(),
            rel.getTraitSet().replace(IgniteConvention.INSTANCE),
            convert(proj.getInput(), proj.getInput().getTraitSet().replace(IgniteConvention.INSTANCE)),
            proj.getProjects(),
            proj.getRowType());
    }

//    public ProjectRule() {
//        super(operand(LogicalProject.class, operand(RelNode.class, any())), "IgniteProjectRule");
//    }
//
//    @Override public void onMatch(RelOptRuleCall call) {
//        final LogicalProject proj = call.rel(0);
//
//        final RelNode input = proj.getInput();
//        RelTraitSet traits = input.getTraitSet().replace(IgniteConvention.INSTANCE);
//
//        RelNode convertedInput = convert(input, traits);
//
//        if (convertedInput instanceof RelSubset) {
//            RelSubset subset = (RelSubset) convertedInput;
//            for (RelNode rel : subset.getRelList()) {
//                if (!rel.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE).equals(RelDistributions.ANY)) {
//                    RelDistribution childDist = rel.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
//                    //RelCollation childCollation = rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
//
//
//                    RelDistribution newDist = convertDist(childDist); //TODO precise distribution handling see Drill's ProjectPrule
//                    //RelCollation newCollation = convertRelCollation(childCollation, inToOut);
//
//                    call.transformTo(new ProjectRel(proj.getCluster(), proj.getTraitSet().plus(newDist).plus(IgniteConvention.INSTANCE),
//                        rel, proj.getProjects(), proj.getRowType()));
//                }
//            }
//
//
////            RelSubset subset = (RelSubset) convertedInput;
////
////            RelNode bestRel = subset.getBest();
////
////            if (bestRel != null) {
////                call.transformTo(new ProjectRel(proj.getCluster(), bestRel.getTraitSet(), convertedInput,proj.getProjects(), proj.getRowType()));
////
////                return;
////            }
//        }
//        else
//            call.transformTo(new ProjectRel(proj.getCluster(),
//                traits.simplify(),
//                convertedInput,
//                proj.getProjects(),
//                proj.getRowType()));
//
//
//    }
//
//    private RelDistribution convertDist(RelDistribution dist) {
//        return dist;
//    }

//    @Override public RelNode convert(RelNode rel) {
//        LogicalProject proj = (LogicalProject)rel;
//
//        return new ProjectRel(rel.getCluster(),
//            rel.getTraitSet().replace(IgniteConvention.INSTANCE),
//            convert(proj.getInput(), proj.getInput().getTraitSet().replace(IgniteConvention.INSTANCE)),
//            proj.getProjects(),
//            proj.getRowType());
//    }
}

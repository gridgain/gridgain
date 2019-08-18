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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.ignite.internal.sql.calcite.IgniteConvention;

/**
 * TODO: Add class description.
 */
/**
 * TODO: Add class description.
 */
public class FilterRule extends ConverterRule {

    public FilterRule() {
        super(LogicalFilter.class,
            Convention.NONE,
            IgniteConvention.INSTANCE,
            "IgniteFilterRule");
    }

    @Override public RelNode convert(RelNode rel) {
        LogicalFilter filter = (LogicalFilter) rel;
        RelNode input = filter.getInput();

        RelTraitSet newTraits = input.getTraitSet().replace(IgniteConvention.INSTANCE);
        RelNode newInput = convert(input, newTraits);

        if (newInput instanceof RelSubset) { // We need to pull distribution trait from child nodes
            RelSubset subset = (RelSubset) newInput;

            for (RelNode r : subset.getRelList()) {
                RelDistribution childDist = r.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);

                if (!childDist.equals(RelDistributions.ANY)) { // TODO check ignite convention?
                    return new FilterRel(rel.getCluster(),
                        newInput.getTraitSet().replace(childDist),
                        newInput,
                        filter.getCondition());
                }
            }
        }

        return new FilterRel(rel.getCluster(),
            newInput.getTraitSet(),
            newInput,
            filter.getCondition());
    }
}

//    public FilterRule() {
//        super(operand(LogicalFilter.class, operand(RelNode.class, RelOptRule.any())), "IgniteFilterRule");
//    }
//
//    @Override public void onMatch(RelOptRuleCall call) {
//        final LogicalFilter filter = call.rel(0);
//
//        final RelNode input = filter.getInput();
//        RelTraitSet traits = filter.getTraitSet().plus(IgniteConvention.INSTANCE);
//
//        RelNode convertedInput = convert(input, traits);
//
//        if (convertedInput instanceof RelSubset) {
//            RelSubset subset = (RelSubset) convertedInput;
//
//            RelNode bestRel = subset.getBest();
//
//            if (bestRel != null) {
//                call.transformTo(new FilterRel(filter.getCluster(), bestRel.getTraitSet(), convertedInput, filter.getCondition()));
//
//                return;
//            }
//        }
//
//        call.transformTo(new FilterRel(filter.getCluster(),
//            traits.simplify(),
//            convertedInput,
//            filter.getCondition()));
//    }

//    @Override public RelNode convert(RelNode rel) {
//
////        inal DrillFilterRel  filter = (DrillFilterRel) call.rel(0);
////        final RelNode input = filter.getChild();
////
////        RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL);
////        RelNode convertedInput = convert(input, traits);
////        boolean transform = false;
////
////        if (convertedInput instanceof RelSubset) {
////            RelSubset subset = (RelSubset) convertedInput;
////            RelNode bestRel = null;
////            if ((bestRel = subset.getBest()) != null) {
////                call.transformTo(new FilterPrel(filter.getCluster(), bestRel.getTraitSet(), convertedInput, filter.getCondition()));
////                transform = true;
////            }
////        }
////        if (!transform) {
////            call.transformTo(new FilterPrel(filter.getCluster(), convertedInput.getTraitSet(), convertedInput, filter.getCondition()));
////        }
////    }
//
//        final LogicalFilter filter = (LogicalFilter) rel;
//        final RelNode input = filter.getInput();
//
//        RelTraitSet traits = input.getTraitSet().replace(IgniteConvention.INSTANCE);
//        RelNode convertedInput = RelOptRule.convert(rel, traits.simplify());
//
//        if (input instanceof RelSubset) {
//            RelSubset subset = (RelSubset) input;
//            RelNode bestRel = subset.getBest();
//            if (bestRel != null)
//                return new FilterRel(filter.getCluster(), bestRel.getTraitSet(), convertedInput, filter.getCondition());
//        }
//
//        return new FilterRel(rel.getCluster(),
//            traits.simplify(),
//            convertedInput, // TODO why do we need to replace a child's convention?
//            filter.getCondition());
//    }
//}

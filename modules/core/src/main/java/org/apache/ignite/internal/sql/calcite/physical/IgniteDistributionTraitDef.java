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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.sql.calcite.IgniteConvention;

/**
 * TODO: Add class description.
 */
public class IgniteDistributionTraitDef extends RelTraitDef<IgniteDistributionTrait> {
    public static final IgniteDistributionTraitDef INSTANCE = new IgniteDistributionTraitDef();

    @Override public Class<IgniteDistributionTrait> getTraitClass() {
        return IgniteDistributionTrait.class;
    }

    @Override public String getSimpleName() {
        return getClass().getSimpleName();
    }

    @Override public IgniteDistributionTrait getDefault() {
        return IgniteDistributionTrait.ANY;
    }

    @Override public boolean canConvert(RelOptPlanner planner, IgniteDistributionTrait fromTrait,
        IgniteDistributionTrait toTrait) {
        return true;
    }

    @Override public RelNode convert(RelOptPlanner planner, RelNode rel, IgniteDistributionTrait toDist,
        boolean allowInfiniteCostConverters) {
        IgniteDistributionTrait currDist = rel.getTraitSet().getTrait(INSTANCE);

        // Source and Target have the same trait.
        if (currDist.equals(toDist)) {
            return rel;
        }

        // Source trait is "ANY", which is abstract type of distribution.
        // We do not want to convert from "ANY", since it's abstract.
        // Source trait should be concrete type: SINGLETON, HASH_DISTRIBUTED, etc.
        if (currDist.equals(IgniteDistributionTrait.ANY) && !(rel instanceof RelSubset)) {
            return null;
        }

        // It is only possible to apply a distribution trait to a DRILL_PHYSICAL convention.
        if (rel.getConvention() != IgniteConvention.INSTANCE) {
            return null;
        }

        switch (toDist.distributionType()) {
            case ANY:
                return rel;

            // UnionExchange, HashToRandomExchange, OrderedPartitionExchange and BroadcastExchange destroy the ordering property,
            // therefore RelCollation is set to default, which is EMPTY.

            case SINGLETON:
                return new UnionExchangeRel(rel.getCluster(), planner.emptyTraitSet().plus(toDist).plus(IgniteConvention.INSTANCE), rel);

            default:
                return null;
        }

    }
}

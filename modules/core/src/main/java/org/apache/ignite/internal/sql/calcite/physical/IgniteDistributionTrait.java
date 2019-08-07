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
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

/**
 * TODO: Add class description.
 */
public class IgniteDistributionTrait implements RelTrait {

    public static final IgniteDistributionTrait SINGLETON = new IgniteDistributionTrait(DistributionType.SINGLETON);
    public static final IgniteDistributionTrait HASH_DISTRIBUTED = new IgniteDistributionTrait(DistributionType.HASH_DISTRIBUTED);
    public static final IgniteDistributionTrait RANGE_DISTRIBUTED = new IgniteDistributionTrait(DistributionType.RANGE_DISTRIBUTED);
    public static final IgniteDistributionTrait BROADCAST_DISTRIBUTED = new IgniteDistributionTrait(DistributionType.BROADCAST_DISTRIBUTED);
    public static final IgniteDistributionTrait ANY = new IgniteDistributionTrait(DistributionType.ANY);


    private final DistributionType distType;

    public IgniteDistributionTrait(DistributionType type) {
        distType = type;
    }

    public DistributionType distributionType() {
        return distType;
    }

    @Override public RelTraitDef getTraitDef() {
        return IgniteDistributionTraitDef.INSTANCE;
    }

    @Override public boolean satisfies(RelTrait trait) {
        if (trait instanceof IgniteDistributionTrait) {
            DistributionType targetType = ((IgniteDistributionTrait)trait).distributionType();

            if (targetType == IgniteDistributionTrait.DistributionType.ANY)
                return true;
        }

        return this.equals(trait);
    }

    @Override public void register(RelOptPlanner planner) {
        // No-op.
    }

    @Override public String toString() {
        return "distTrait{" + distType +
            '}';
    }

    public enum DistributionType {
        SINGLETON,
        HASH_DISTRIBUTED,
        RANGE_DISTRIBUTED,
        BROADCAST_DISTRIBUTED,
        ANY};
}

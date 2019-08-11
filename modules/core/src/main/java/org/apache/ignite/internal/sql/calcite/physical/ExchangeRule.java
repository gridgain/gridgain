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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.ignite.internal.sql.calcite.IgniteConvention;

/**
 * TODO: Add class description.
 */
public class ExchangeRule extends IgniteRule {

    public ExchangeRule() {
        super(operand(LogicalExchange.class, operand(RelNode.class, RelOptRule.any())), "IgniteExchangeRule");
    }

    @Override public void onMatch(RelOptRuleCall call) {
        final LogicalExchange logicalExchange = call.rel(0);

        final RelNode input = logicalExchange.getInput();

        RelTraitSet traits = input.getTraitSet().replace(IgniteConvention.INSTANCE).replace(logicalExchange.getDistribution());

        IgniteRel exchange;

        RelDistribution.Type distType = logicalExchange.getDistribution().getType();


        switch (distType) {
            case SINGLETON:
                exchange = new UnionExchangeRel(logicalExchange.getCluster(), traits.simplify(), input);

                break;

            case HASH_DISTRIBUTED:
                exchange = new RehashingExchange(logicalExchange.getDistribution(), logicalExchange.getCluster(), traits.simplify(), input);

                break;

            default:
               return;
        }

        call.transformTo(exchange);
    }
}

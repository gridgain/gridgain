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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.ignite.internal.sql.calcite.IgniteConvention;

/**
 * TODO: Add class description.
 */
public class ExchangeRule extends ConverterRule {

    public <R extends RelNode> ExchangeRule() {
        super(LogicalExchange.class, IgniteDistributionTrait.HASH_DISTRIBUTED, IgniteDistributionTrait.SINGLETON,  "ExchangeRule");
    }

    @Override public RelNode convert(RelNode rel) {
        LogicalExchange exch = (LogicalExchange)rel;

        return new UnionExchangeRel(exch.getCluster(),
            exch.getTraitSet().replace(IgniteConvention.INSTANCE).replace(IgniteDistributionTrait.SINGLETON),
            exch.getInput()); // TODO: CODE: implement.
    }
}

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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.ignite.internal.sql.calcite.IgniteConvention;

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
        final LogicalFilter filter = (LogicalFilter) rel;

        return new FilterRel(rel.getCluster(),
            rel.getTraitSet().replace(IgniteConvention.INSTANCE),
            convert(filter.getInput(),
                filter.getInput().getTraitSet().replace(IgniteConvention.INSTANCE)), // TODO why do we need to replace a child's convention?
            filter.getCondition());
    }
}

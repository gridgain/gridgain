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
package org.apache.ignite.internal.sql.calcite.plan;

import org.apache.calcite.rel.core.Join;
import org.apache.ignite.internal.sql.calcite.expressions.Condition;
import org.apache.ignite.internal.sql.calcite.rels.FilterRel;
import org.apache.ignite.internal.sql.calcite.rels.IgniteRel;
import org.apache.ignite.internal.sql.calcite.rels.IgniteRelVisitor;
import org.apache.ignite.internal.sql.calcite.rels.ProjectRel;
import org.apache.ignite.internal.sql.calcite.rels.RehashingExchange;
import org.apache.ignite.internal.sql.calcite.rels.TableScanRel;
import org.apache.ignite.internal.sql.calcite.rels.UnionExchangeRel;

import static org.apache.ignite.internal.sql.calcite.expressions.Condition.buildFilterCondition;

/**
 * TODO: Add class description.
 */
public class PlanSplitter implements IgniteRelVisitor {

    private SubPlan currSubPlan = new SubPlan();


    @Override public void onFilter(FilterRel filter) {
        IgniteRel relInput = (IgniteRel)filter.getInput();

        Condition cond = (Condition)buildFilterCondition(filter.getCondition());
        FilterNode node = new FilterNode(cond);

        relInput.accept(this);

    }

    @Override public void onJoin(Join join) {

    }

    @Override public void onProject(ProjectRel project) {

    }

    @Override public void onTableScan(TableScanRel scan) {

    }

    @Override public void onUnionExchange(UnionExchangeRel exch) {

    }

    @Override public void onRehashingExchange(RehashingExchange exch) {

    }


}

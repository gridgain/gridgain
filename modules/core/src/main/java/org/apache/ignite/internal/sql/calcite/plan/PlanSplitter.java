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

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.sql.calcite.IgniteTable;
import org.apache.ignite.internal.sql.calcite.expressions.Condition;
import org.apache.ignite.internal.sql.calcite.rels.FilterRel;
import org.apache.ignite.internal.sql.calcite.rels.IgniteRel;
import org.apache.ignite.internal.sql.calcite.rels.IgniteRelVisitor;
import org.apache.ignite.internal.sql.calcite.rels.ProjectRel;
import org.apache.ignite.internal.sql.calcite.rels.RehashingExchange;
import org.apache.ignite.internal.sql.calcite.rels.TableScanRel;
import org.apache.ignite.internal.sql.calcite.rels.UnionExchangeRel;

import static org.apache.ignite.internal.sql.calcite.expressions.Condition.buildFilterCondition;
import static org.apache.ignite.internal.sql.calcite.plan.JoinNode.JoinAlgorithm.NESTED_LOOPS;

/**
 * Runs post-order tree traversing and splits the tree when visiting exchange node.
 */
@SuppressWarnings("TypeMayBeWeakened") public class PlanSplitter implements IgniteRelVisitor {

    private int curSubPlanId;

    private List<SubPlan> subPlans = new ArrayList<>();


    private Deque<PlanNode> childrenStack = new LinkedList<>();

    @Override public void onTableScan(TableScanRel scan) {
        IgniteTable tbl = scan.table();

        TableScanNode node = new TableScanNode(tbl.tableName(), tbl.cacheName())

        childrenStack.push(node);
    }

    @Override public void onFilter(FilterRel filter) {
        visitChildren(filter);

        PlanNode input = childrenStack.poll();

        Condition cond = (Condition)buildFilterCondition(filter.getCondition());

        FilterNode node = new FilterNode(cond, input);

        childrenStack.push(node);
    }


    @Override public void onJoin(Join join) {
        visitChildren(join);

        PlanNode right = childrenStack.poll();
        PlanNode left = childrenStack.poll();

        JoinInfo info = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());

        JoinNode node = new JoinNode(left, right, info.leftKeys.toIntArray(), info.rightKeys.toIntArray(),
            (Condition)buildFilterCondition(join.getCondition()), join.getJoinType(), NESTED_LOOPS); // TODO Other types.

        childrenStack.push(node);

    }

    @Override public void onProject(ProjectRel project) {
        visitChildren(project);

        PlanNode input = childrenStack.poll();

        int[] projections = new int[project.getProjects().size()];

        int i = 0;

        for (RexNode proj : project.getProjects())
            projections[i++] = ((RexSlot)proj).getIndex();

        ProjectNode node = new ProjectNode(projections, input);

        childrenStack.push(node);
    }


    @Override public void onUnionExchange(UnionExchangeRel exch) {
        visitChildren(exch);

        PlanNode input = childrenStack.poll();

        SenderNode sender = new SenderNode(input, SenderNode.SenderType.SINGLE);

        SubPlan subPlan = nextSubPlan(sender);

        subPlans.add(subPlan);


    }

    @Override public void onRehashingExchange(RehashingExchange exch) {

    }

    private void visitChildren(RelNode rel) {
        for (RelNode input : rel.getInputs())
            ((IgniteRel)input).accept(this);
    }


    private SubPlan nextSubPlan(PlanNode subPlan) {
        return new SubPlan(++curSubPlanId, subPlan);
    }
}

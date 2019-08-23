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
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.ignite.internal.sql.calcite.IgniteTable;
import org.apache.ignite.internal.sql.calcite.expressions.Condition;
import org.apache.ignite.internal.sql.calcite.rels.FilterRel;
import org.apache.ignite.internal.sql.calcite.rels.IgniteRel;
import org.apache.ignite.internal.sql.calcite.rels.IgniteRelVisitor;
import org.apache.ignite.internal.sql.calcite.rels.OutputRel;
import org.apache.ignite.internal.sql.calcite.rels.ProjectRel;
import org.apache.ignite.internal.sql.calcite.rels.RehashingExchange;
import org.apache.ignite.internal.sql.calcite.rels.TableScanRel;
import org.apache.ignite.internal.sql.calcite.rels.UnionExchangeRel;

import static org.apache.ignite.internal.sql.calcite.expressions.Condition.buildFilterCondition;
import static org.apache.ignite.internal.sql.calcite.plan.JoinNode.JoinAlgorithm.NESTED_LOOPS;

/**
 * Runs post-order tree traversing and splits the tree when visiting exchange node.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class PlanSplitter implements IgniteRelVisitor {

    private int curSubPlanId = 1; // Let's start enumeration from 1.

    private List<PlanStep> planSteps = new ArrayList<>();

    private Deque<PlanNode> childrenStack = new LinkedList<>();

    public List<PlanStep> subPlans() {
        return planSteps;
    }

    @Override public void onTableScan(TableScanRel scan) {
        IgniteTable tbl = scan.table();

        TableScanNode node = new TableScanNode(tbl.tableName(), tbl.cacheName());

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

    @Override public void onOutput(OutputRel out) {
        visitChildren(out);

        PlanNode input = childrenStack.poll();

        OutputNode outputNode = new OutputNode(input);

        PlanStep planStep = new PlanStep(curSubPlanId, outputNode, PlanStep.Site.SINGLE_NODE);

        planSteps.add(planStep);
    }

    @Override public void onUnionExchange(UnionExchangeRel exch) {
        visitChildren(exch);

        RelDistribution dist = exch.getInput().getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);

        boolean singleSrc = dist.satisfies(RelDistributions.SINGLETON);

        PlanNode input = childrenStack.poll();

        int linkId = curSubPlanId++;

        SenderNode snd = new SenderNode(input, SenderNode.SenderType.SINGLE, linkId, null);

        PlanStep.Site site = singleSrc ? PlanStep.Site.SINGLE_NODE : PlanStep.Site.ALL_NODES;

        PlanStep planStep = new PlanStep(linkId, snd, site);

        planSteps.add(planStep);

        ReceiverNode receiver = new ReceiverNode(linkId, singleSrc ? ReceiverNode.Type.SINGLE :  ReceiverNode.Type.ALL);

        childrenStack.push(receiver);
    }

    @Override public void onRehashingExchange(RehashingExchange exch) { // TODO Add ROOT (Outcome?) node
        visitChildren(exch);

        PlanNode input = childrenStack.poll();

        int linkId = curSubPlanId++;

        RelDistribution dist = exch.getInput().getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);

        SenderNode snd = new SenderNode(input, SenderNode.SenderType.HASH, linkId,  dist.getKeys());

        PlanStep planStep = new PlanStep(linkId, snd, PlanStep.Site.ALL_NODES);

        planSteps.add(planStep);

        ReceiverNode receiver = new ReceiverNode(linkId, ReceiverNode.Type.ALL);

        childrenStack.push(receiver);
    }

    private void visitChildren(RelNode rel) {
        for (RelNode input : rel.getInputs())
            ((IgniteRel)input).accept(this);
    }
}

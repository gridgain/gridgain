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
package org.apache.ignite.internal.sql.calcite.iterators;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.sql.calcite.executor.ExecutorOfGovnoAndPalki;
import org.apache.ignite.internal.sql.calcite.plan.FilterNode;
import org.apache.ignite.internal.sql.calcite.plan.JoinNode;
import org.apache.ignite.internal.sql.calcite.plan.OutputNode;
import org.apache.ignite.internal.sql.calcite.plan.PlanNode;
import org.apache.ignite.internal.sql.calcite.plan.ProjectNode;
import org.apache.ignite.internal.sql.calcite.plan.ReceiverNode;
import org.apache.ignite.internal.sql.calcite.plan.SenderNode;
import org.apache.ignite.internal.sql.calcite.plan.TableScanNode;
import org.apache.ignite.internal.sql.calcite.rels.IgnitePlanVisitor;
import org.apache.ignite.internal.util.typedef.T2;

/**
 * TODO: Add class description.
 */
public class PhysicalPlanCreator implements IgnitePlanVisitor {
    private List<ReceiverOp> receivers = new ArrayList<>();

    private Deque<PhysicalOperator> childrenStack = new LinkedList<>();

    private final int totalDataNodeCnt;

    private final T2<UUID, Long> qryId;
    private final ExecutorOfGovnoAndPalki exec;

    public PhysicalPlanCreator(int totalDataNodeCnt, T2<UUID, Long> qryId, ExecutorOfGovnoAndPalki exec) {
        this.totalDataNodeCnt = totalDataNodeCnt;
        this.qryId = qryId;
        this.exec = exec;
    }

    @Override public void onFilter(FilterNode filter) {
        visitChildren(filter);

        PhysicalOperator srcOp = childrenStack.poll();

        FilterOp filterOp = new FilterOp(srcOp, filter.filterCondition());

        childrenStack.push(filterOp);
    }

    @Override public void onJoin(JoinNode join) {
        visitChildren(join);

        PhysicalOperator right = childrenStack.poll();
        PhysicalOperator left = childrenStack.poll();

        NestedLoopsJoinOp joinOp = new NestedLoopsJoinOp(left, right, join.leftJoinKeys(), join.rightJoinKeys(),
            join.joinCond(), join.joinType());

        childrenStack.push(joinOp);
    }

    @Override public void onOutput(OutputNode output) {
        visitChildren(output);

        PhysicalOperator srcOp = childrenStack.poll();

        OutputOp outputNode = new OutputOp(srcOp);

        childrenStack.push(outputNode);
    }

    @Override public void onProject(ProjectNode project) {
        visitChildren(project);

        PhysicalOperator srcOp = childrenStack.poll();

        ProjectOp filterOp = new ProjectOp(srcOp, project.projectIndexes());

        childrenStack.push(filterOp);
    }

    @Override public void onReceiver(ReceiverNode receiver) {
        visitChildren(receiver);

        int dataNodes = receiver.type() == ReceiverNode.Type.SINGLE ? 1 : totalDataNodeCnt;

        ReceiverOp receiverOp = new ReceiverOp(dataNodes, receiver.inputLink());

        childrenStack.push(receiverOp);

        receivers.add(receiverOp);
    }

    @Override public void onSender(SenderNode sender) {
        visitChildren(sender);

        PhysicalOperator srcOp = childrenStack.poll();

        SenderOp senderOp = new SenderOp(srcOp, sender.type(), sender.linkId(), qryId, sender.distKeys(), exec);

        childrenStack.push(senderOp);

    }

    @Override public void onTableScan(TableScanNode scan) {
        visitChildren(scan);
        
        TableScanOp scanOp = new TableScanOp(exec.table(scan.tableName()), exec.cache(scan.cacheName()));

        childrenStack.push(scanOp);
    }

    public PhysicalOperator root() {
        assert childrenStack.size() == 1;

        return childrenStack.peek();
    }

    public List<ReceiverOp> receivers() {
        return receivers;
    }

    private void visitChildren(PlanNode node) {
        for (PlanNode child : node.inputs())
            child.accept(this);
    }
}

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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.sql.calcite.rels.IgnitePlanVisitor;

/**
 * TODO: Add class description.
 */
public class TableScanNode implements PlanNode {

    private String tableName;

    private String cacheName;

    public TableScanNode() {
    }

    public TableScanNode(String tableName, String cacheName) {
        this.tableName = tableName;
        this.cacheName = cacheName;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(tableName);
        out.writeUTF(cacheName);

    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tableName = in.readUTF();
        cacheName = in.readUTF();
    }


    @Override public String toString(int level) {
        String margin = String.join("", Collections.nCopies(level, "  "));

        StringBuilder sb = new StringBuilder("\n");

        sb.append(margin)
            .append("TableScanNode [tableName=")
            .append(tableName)
            .append("]");

        return sb.toString();
    }

    @Override public void accept(IgnitePlanVisitor visitor) {
        visitor.onTableScan(this);
    }

    @Override public List<PlanNode> inputs() {
        return Collections.emptyList();
    }

    public String tableName() {
        return tableName;
    }

    public String cacheName() {
        return cacheName;
    }

    @Override public String toString() {
        return toString(0);
    }
}

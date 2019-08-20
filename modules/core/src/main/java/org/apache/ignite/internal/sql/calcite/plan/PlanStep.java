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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * TODO: Add class description.
 */
public class PlanStep implements Externalizable {
    private int id;

    private PlanNode planStep;

    private Distribution dist;


    public PlanStep(int id, PlanNode planStep, Distribution dist) {
        this.id = id;
        this.planStep = planStep;
        this.dist = dist;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(dist.ordinal());
        out.writeObject(planStep);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readInt();
        dist = Distribution.values()[in.readInt()];
        planStep = (PlanNode)in.readObject();
    }

    public int id() {
        return id;
    }

    public PlanNode plan() {
        return planStep;
    }

    public Distribution distribution() {
        return dist;
    }

    public enum Distribution {
        SINGLE_NODE,
        ALL_NODES
    }

    @Override public String toString() {
        return "PlanStep{" +
            "id=" + id +
            ", plan=" + planStep +
            ", dist=" + dist +
            '}';
    }
}

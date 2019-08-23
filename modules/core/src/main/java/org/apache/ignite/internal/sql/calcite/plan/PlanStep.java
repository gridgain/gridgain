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

    private Site site;

    public PlanStep() {
    }

    public PlanStep(int id, PlanNode planStep, Site site) {
        this.id = id;
        this.planStep = planStep;
        this.site = site;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(site.ordinal());
        out.writeObject(planStep);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readInt();
        site = Site.values()[in.readInt()];
        planStep = (PlanNode)in.readObject();
    }

    public int id() {
        return id;
    }

    public PlanNode plan() {
        return planStep;
    }

    public Site distribution() {
        return site;
    }

    public enum Site {
        SINGLE_NODE,
        ALL_NODES
    }

    @Override public String toString() {
        return "\nPlanStep{" +
            "id=" + id +
            ", site=" + site +
            ", plan=" + planStep +
            '}';
    }
}

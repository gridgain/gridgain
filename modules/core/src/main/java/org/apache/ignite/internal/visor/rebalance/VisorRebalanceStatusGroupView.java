/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.visor.rebalance;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * View of group.
 */
public class VisorRebalanceStatusGroupView extends IgniteDataTransferObject {

    /** Group id. */
    private Integer id;

    /** Group name. */
    private String name;

    /** Flag means that group rebalanced or in process of rebalance. */
    private boolean isRebalanceComplited;

    /**
     * Default constructor.
     */
    public VisorRebalanceStatusGroupView() {
    }

    /**
     * @param id Group id.
     * @param name Group name.
     * @param isRebalanceComplited Rebalanced flag.
     */
    public VisorRebalanceStatusGroupView(Integer id, String name, boolean isRebalanceComplited) {
        this.id = id;
        this.name = name;
        this.isRebalanceComplited = isRebalanceComplited;
    }

    /**
     * @return Group id.
     */
    public Integer getId() {
        return id;
    }

    /**
     * @return Group name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Rebalanced flag.
     */
    public boolean isRebalanceComplited() {
        return isRebalanceComplited;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);
        out.writeBoolean(isRebalanceComplited);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readInt();
        name = in.readUTF();
        isRebalanceComplited = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorRebalanceStatusGroupView.class, this);
    }
}

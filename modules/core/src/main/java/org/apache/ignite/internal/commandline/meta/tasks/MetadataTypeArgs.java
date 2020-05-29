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

package org.apache.ignite.internal.commandline.meta.tasks;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class MetadataTypeArgs extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Config. */
    private String typeName;

    /** Metrics. */
    private Integer typeId;

    /**
     * Default constructor.
     */
    public MetadataTypeArgs() {
        // No-op.
    }

    /** */
    public MetadataTypeArgs(String typeName, Integer typeId) {
        assert typeName != null ^ typeId != null;

        this.typeName = typeName;
        this.typeId = typeId;
    }

    /** */
    public String typeName() {
        return typeName;
    }

    /** */
    public Integer typeId() {
        return typeId;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(typeName != null);

        if (typeName != null)
            U.writeString(out, typeName);
        else
            out.writeInt(typeId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        boolean useName = in.readBoolean();

        if (useName)
            typeName = U.readString(in);
        else
            typeId = in.readInt();
    }
}

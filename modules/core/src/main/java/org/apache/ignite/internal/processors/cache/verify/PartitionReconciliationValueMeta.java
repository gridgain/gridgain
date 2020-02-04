/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.verify;

import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class PartitionReconciliationValueMeta extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    @GridToStringInclude
    private byte[] binaryView;

    private String strView;

    /**
     * Default constructor for externalization.
     */
    public PartitionReconciliationValueMeta() {
    }

    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public PartitionReconciliationValueMeta(byte[] binaryView, String strView) {
        this.binaryView = binaryView;
        this.strView = strView;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeByteArray(out, binaryView);
        U.writeString(out, strView);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        binaryView = U.readByteArray(in);
        strView = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionReconciliationValueMeta.class, this);
    }
}

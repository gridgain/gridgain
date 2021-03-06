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

package org.apache.ignite.internal.visor.encryption;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Cache group aware task argument.
 */
public class VisorCacheGroupEncryptionTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache group name. */
    private String grpName;

    /** Default constructor. */
    public VisorCacheGroupEncryptionTaskArg() {
        // No-op.
    }

    /**
     * @param grpName Cache group name.
     */
    public VisorCacheGroupEncryptionTaskArg(String grpName) {
        this.grpName = grpName;
    }

    /** @return Cache group name. */
    public String groupName() {
        return grpName;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, grpName);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte ver, ObjectInput in) throws IOException, ClassNotFoundException {
        grpName = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheGroupEncryptionTaskArg.class, this);
    }
}

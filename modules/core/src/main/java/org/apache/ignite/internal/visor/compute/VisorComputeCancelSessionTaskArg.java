/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.visor.compute;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Arguments for task {@link VisorComputeCancelSessionTask}
 */
public class VisorComputeCancelSessionTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Session IDs to cancel. */
    private IgniteUuid sesId;

    /**
     * Default constructor.
     */
    public VisorComputeCancelSessionTaskArg() {
        // No-op.
    }

    /**
     * @param sesId Session IDs to cancel.
     */
    public VisorComputeCancelSessionTaskArg(IgniteUuid sesId) {
        this.sesId = sesId;
    }

    /**
     * @return Session IDs to cancel.
     */
    public IgniteUuid getSessionId() {
        return sesId;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, sesId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        sesId = U.readGridUuid(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorComputeCancelSessionTaskArg.class, this);
    }
}

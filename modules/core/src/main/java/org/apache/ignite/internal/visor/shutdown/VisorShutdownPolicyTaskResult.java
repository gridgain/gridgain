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

package org.apache.ignite.internal.visor.shutdown;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/**
 * Shutdown policy visor trsk result.
 */
public class VisorShutdownPolicyTaskResult extends IgniteDataTransferObject {
    /** Serial version id. */
    private static final long serialVersionUID = 0L;

    /** Shutdown policy on result. */
    private ShutdownPolicy shutdown;

    /**
     * Get policy.
     *
     * @return Shutdown policy.
     */
    public ShutdownPolicy getShutdown() {
        return shutdown;
    }

    /**
     * Set policy.
     *
     * @param shutdown Shutdown policy.
     */
    public void setShutdown(ShutdownPolicy shutdown) {
        this.shutdown = shutdown;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(shutdown == null ? -1 : shutdown.index());
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        shutdown = ShutdownPolicy.fromOrdinal(in.readInt());

    }
}

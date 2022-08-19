/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.visor.checkpoint;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result of CheckpointTask
 */
public class VisorCheckpointTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Status of force checkpointing */
    private Map<UUID, VisorCheckpointJobResult> status = new HashMap<>();

    /**
     * Whether the checkpoint on all nodes is completed successfully
     *
     * @return is checkpointing successful
     */
    public boolean isSuccess() {
        return 0 == numberOfFailedNodes();
    }

    /**
     * Return number of nodes where checkpointing was failed
     *
     * @return number of nodes
     */
    public long numberOfFailedNodes() {
        return status.values().stream().filter(VisorCheckpointJobResult::failed).count();
    }

    /**
     * Return number of nodes with checkpoint completed successfully
     *
     * @return number of nodes
     */
    public long numberOfSuccessNodes() {
        return status.values().stream().filter(VisorCheckpointJobResult::success).count();
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, status);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        status = U.readMap(in);
    }

    /**
     * Returns checkpoint status for all involved server nodes.
     *
     * @return Checkpoint status.
     */
    public Map<UUID, VisorCheckpointJobResult> status() {
        return status;
    }
}

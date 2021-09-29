/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.commandline.checkpointing;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.task.GridInternal;

/**
 *
 */
@GridInternal
public class NodeCheckpointingResult extends IgniteDataTransferObject {

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private UUID nodeId;

    /** */
    private long durationMillis;

    /** */
    private Throwable error;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(nodeId);
        out.writeLong(durationMillis);
        out.writeObject(error);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = (UUID)in.readObject();
        durationMillis = in.readLong();
        error = (Exception)in.readObject();
    }

    /**
     * @return Node id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Success.
     */
    public boolean success() {
        return null == error;
    }

    /**
     *
     * @return is checkpoint failed
     */
    public boolean failed() {
        return !success();
    }

    /**
     * @return Duration millis.
     */
    public long durationMillis() {
        return durationMillis;
    }

    /**
     * @return Error.
     */
    public Throwable error() {
        return error;
    }

    /**
     * @param nodeId New node id.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @param durationMillis New duration millis.
     */
    public void durationMillis(long durationMillis) {
        this.durationMillis = durationMillis;
    }

    /**
     * @param error New error.
     */
    public void error(Throwable error) {
        this.error = error;
    }
}

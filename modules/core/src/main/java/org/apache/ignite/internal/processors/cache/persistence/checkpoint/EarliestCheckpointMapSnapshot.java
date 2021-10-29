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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Earliest checkpoint map snapshot.
 * Speeds up construction of the earliestCp map in the {@link CheckpointHistory}.
 */
public class EarliestCheckpointMapSnapshot implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Class protocol version. */
    private static final int PROTO_VER = 1;

    /** Protocol version of the object. */
    private int ver = PROTO_VER;

    /** Last snapshot's checkpoint timestamp. */
    private Map</*Checkpoint id */ UUID, Map</* Group id */ Integer, CheckpointEntry.GroupState>> data = new HashMap<>();

    /** Ids of checkpoints present at the time of the snapshot capture. */
    private Set<UUID> checkpointIds;

    /** Constructor. */
    public EarliestCheckpointMapSnapshot(
        Set<UUID> checkpointIds,
        Map<UUID, Map<Integer, CheckpointEntry.GroupState>> earliestCp
    ) {
        this.checkpointIds = checkpointIds;
        this.data = earliestCp;
    }

    /** Default constructor. */
    public EarliestCheckpointMapSnapshot() {
        checkpointIds = new HashSet<>();
    }

    /**
     * Gets a group state by a checkpoint id.
     *
     * @param checkpointId Checkpoint id.
     * @return Group state.
     */
    @Nullable
    public Map<Integer, CheckpointEntry.GroupState> getGroupState(UUID checkpointId) {
        return data.get(checkpointId);
    }

    /**
     * Returns {@code true} if a checkpoint was present during the snapshot capture, {@code false} otherwise.
     *
     * @param checkpointId Checkpoint id.
     * @return {@code true} if checkpoint was present, {@code false} otherwise.
     */
    public boolean checkpointWasPresent(UUID checkpointId) {
        return checkpointIds.contains(checkpointId);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(ver);

        U.writeMap(out, data);
        U.writeCollection(out, checkpointIds);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ver = in.readInt();

        data = U.readMap(in);
        checkpointIds = U.readSet(in);
    }
}

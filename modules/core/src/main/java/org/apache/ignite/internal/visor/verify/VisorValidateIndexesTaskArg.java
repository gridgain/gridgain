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

package org.apache.ignite.internal.visor.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class VisorValidateIndexesTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Caches. */
    private Set<String> caches;

    /** Check first K elements. */
    private int checkFirst;

    /** Check through K element (skip K-1, check Kth). */
    private int checkThrough;

    /** Nodes on which task will run. */
    private Set<UUID> nodes;

    /** Check crc sums of pages in index partition. */
    private boolean checkCrc;

    /**
     * Default constructor.
     */
    public VisorValidateIndexesTaskArg() {
        // No-op.
    }

    /**
     * @param caches Checked caches, If {@code null} all user caches will be checked.
     * @param nodes Nodes on which task will run. If {@code null}, task will run on all server nodes.
     * @param checkFirst Check only first {@code checkFirst} elements, if {@code checkFirst > 0}.
     * @param checkThrough Check each {@code checkThrough} element (skip {@code checkThrough - 1}, check {@code
     * checkThrough}), if {@code checkThrough > 0}.
     * @param checkCrc Check crc sums of pages in index partition, if {@code True}.
     */
    public VisorValidateIndexesTaskArg(
        @Nullable Set<String> caches,
        @Nullable Set<UUID> nodes,
        int checkFirst,
        int checkThrough,
        boolean checkCrc
    ) {
        this.caches = caches;
        this.checkFirst = checkFirst;
        this.checkThrough = checkThrough;
        this.nodes = nodes;
        this.checkCrc = checkCrc;
    }


    /**
     * @return Checked caches, If {@code null} all user caches will be checked.
     */
    public @Nullable Set<String> getCaches() {
        return caches;
    }

    /**
     * @return Nodes on which task will run. If {@code null}, task will run on all server nodes.
     */
    public @Nullable Set<UUID> getNodes() {
        return nodes;
    }

    /**
     * @return checkFirst.
     */
    public int getCheckFirst() {
        return checkFirst;
    }

    /**
     * @return checkThrough.
     */
    public int getCheckThrough() {
        return checkThrough;
    }

    /**
     * @return Check crc sums flag.
     */
    public boolean isCheckCrc() {
        return checkCrc;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, caches);
        out.writeInt(checkFirst);
        out.writeInt(checkThrough);
        U.writeCollection(out, nodes);
        out.writeBoolean(checkCrc);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        caches = U.readSet(in);

        if (protoVer > V1) {
            checkFirst = in.readInt();
            checkThrough = in.readInt();
        }
        else {
            checkFirst = -1;
            checkThrough = -1;
        }

        if (protoVer > V2)
            nodes = U.readSet(in);

        if (protoVer > V3)
            checkCrc = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V4;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorValidateIndexesTaskArg.class, this);
    }
}

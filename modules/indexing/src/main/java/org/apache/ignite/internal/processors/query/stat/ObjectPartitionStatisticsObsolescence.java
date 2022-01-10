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
package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.internal.processors.query.stat.hll.HLL;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Partition obsolescence tracker.
 */
public class ObjectPartitionStatisticsObsolescence implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** HLL to track modified keys. */
    private HLL modified;

    /** Dirty flag, {@code true} if there are not saved on disc changes. */
    private transient volatile boolean dirty;

    /**
     * Constructor.
     */
    public ObjectPartitionStatisticsObsolescence() {
        modified = new HLL(13, 5);
    }

    /**
     * @param key Save specified key in modified keys counter.
     */
    public synchronized void onModified(byte[] key) {
        Hasher h = new Hasher();

        modified.addRaw(h.fastHash(key));

        dirty = true;
    }

    /**
     * @return The estimated number of modified keys.
     */
    public synchronized long modified() {
        return modified.cardinality();
    }

    /**
     * @return {@code true} if object has unsaved changes, {@code false} - otherwise.
     */
    public boolean dirty() {
        return dirty;
    }

    /**
     * Set dirty flag.
     *
     * @param dirty The new dirty flag value.
     */
    public void dirty(boolean dirty) {
        this.dirty = dirty;
    }


    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        byte[] rawData = modified.toBytes();

        out.writeInt(rawData.length);
        out.write(rawData);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int length = in.readInt();
        byte[] rawData = new byte[length];
        in.read(rawData);
        modified = HLL.fromBytes(rawData);
    }
}

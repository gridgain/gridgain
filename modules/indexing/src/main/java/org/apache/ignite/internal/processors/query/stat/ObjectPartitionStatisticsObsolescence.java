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

import java.io.Serializable;

/**
 * Partition obsolescence tracker.
 */
public class ObjectPartitionStatisticsObsolescence implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Related statistics version. */
    private final long ver;

    /** HLL to track modified keys. */
    private final HLL modified;

    /** Dirty flag, {@code true} if there are not saved on disc changes. */
    private transient volatile boolean dirty;

    /**
     * Constructor.
     *
     * @param ver Related statistics version.
     */
    public ObjectPartitionStatisticsObsolescence(long ver) {
        this.ver = ver;
        modified = new HLL(13, 5);
    }

    /**
     * Constructor.
     *
     * @param ver
     * @param modified
     */
    public ObjectPartitionStatisticsObsolescence(long ver, HLL modified) {
        this.ver = ver;
        this.modified = modified;
    }

    /**
     * @param key Save specified key in modified keys counter.
     */
    public synchronized void modify(byte[] key) {
        Hasher h = new Hasher();

        modified.addRaw(h.fastHash(key));

        dirty = true;
    }

    /**
     * @return Related statistics version.
     */
    public long ver() {
        return ver;
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
     * Set dirty flag
     *
     * @param dirty The new dirty flag value.
     */
    public void dirty(boolean dirty) {
        this.dirty = dirty;
    }
}

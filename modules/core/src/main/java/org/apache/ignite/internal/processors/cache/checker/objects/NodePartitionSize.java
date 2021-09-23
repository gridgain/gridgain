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

package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/** Container for cache size consistency reconciliation result. */
public class NodePartitionSize extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /** Size reconciliation state. */
    private SizeReconciliationState state;

    /** Broken cache size from partition meta. */
    private long oldCacheSize;

    /** Real cache size. */
    private long newCacheSize;

    /** */
    public NodePartitionSize() {
    }

    /** */
    public NodePartitionSize(String cacheName) {
        this.cacheName = cacheName;
    }

    /** */
    public String cacheName() {
        return cacheName;
    }

    /** */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /** */
    public SizeReconciliationState state() {
        return state;
    }

    /** */
    public void state(SizeReconciliationState state) {
        this.state = state;
    }

    /** */
    public long oldCacheSize() {
        return oldCacheSize;
    }

    /** */
    public void oldCacheSize(long oldCacheSize) {
        this.oldCacheSize = oldCacheSize;
    }

    /** */
    public long newCacheSize() {
        return newCacheSize;
    }

    /** */
    public void newCacheSize(long newCacheSize) {
        this.newCacheSize = newCacheSize;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(cacheName);
        out.writeObject(state);
        out.writeLong(oldCacheSize);
        out.writeLong(newCacheSize);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        cacheName = (String)in.readObject();
        state = (SizeReconciliationState)in.readObject();
        oldCacheSize = in.readLong();
        newCacheSize = in.readLong();
    }

    /** */
    public enum SizeReconciliationState {
        /** */
        NOT_STARTED,

        /** */
        IN_PROGRESS,

        /** */
        FINISHED
    }

    @Override public String toString() {
        return "NodePartitionSize{" +
            "cacheName='" + cacheName + '\'' +
            ", state=" + state +
            ", oldCacheSize=" + oldCacheSize +
            ", newCacheSize=" + newCacheSize +
            '}';
    }
}

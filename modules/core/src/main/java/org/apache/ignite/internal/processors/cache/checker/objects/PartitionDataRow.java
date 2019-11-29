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

package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 *
 */
public class PartitionDataRow extends PartitionKeyVersion {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value. */
    private CacheObject val;

    /**
     *
     */
    private long updateCounter;

    /**
     *
     */
    private long recheckStartTime;

    /**
     *
     */
    public PartitionDataRow() {
    }

    /**
     *
     */
    public PartitionDataRow(UUID nodeId, KeyCacheObject key, GridCacheVersion ver, CacheObject val, long updateCounter,
        long recheckStartTime) {
        super(nodeId, key, ver);
        this.val = val;
        this.updateCounter = updateCounter;
        this.recheckStartTime = recheckStartTime;
    }

    /**
     *
     */
    public CacheObject getVal() {
        return val;
    }

    /**
     *
     */
    public long getUpdateCounter() {
        return updateCounter;
    }

    /**
     *
     */
    public long getRecheckStartTime() {
        return recheckStartTime;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);
        out.writeObject(val);
        out.writeLong(updateCounter);
        out.writeLong(recheckStartTime);

    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(protoVer, in);
        val = (CacheObject)in.readObject();
        updateCounter = in.readLong();
        recheckStartTime = in.readLong();
    }
}

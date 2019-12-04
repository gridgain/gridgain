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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * The data object describes a version of key stored at a node.
 */
public class PartitionKeyVersion extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node id. */
    private UUID nodeId;

    /** Key. */
    private KeyCacheObject key;

    /** Version. */
    private GridCacheVersion version;

    /**
     *
     */
    public PartitionKeyVersion() {
    }

    /**
     * @param nodeId Node id.
     * @param key Key.
     * @param ver Version.
     */
    public PartitionKeyVersion(UUID nodeId, KeyCacheObject key, GridCacheVersion ver) {
        this.nodeId = nodeId;
        this.key = key;
        this.version = ver;
    }

    /**
     *
     */
    public UUID getNodeId() {
        return nodeId;
    }

    /**
     *
     */
    public KeyCacheObject getKey() {
        return key;
    }

    /**
     *
     */
    public GridCacheVersion getVersion() {
        return version;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(nodeId);
        out.writeObject(key);
        out.writeObject(version);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = (UUID)in.readObject();
        key = (KeyCacheObject)in.readObject();
        version = (GridCacheVersion)in.readObject();
    }
}

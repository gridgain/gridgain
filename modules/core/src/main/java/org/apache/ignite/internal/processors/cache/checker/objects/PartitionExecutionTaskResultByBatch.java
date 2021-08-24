/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/** */
public class PartitionExecutionTaskResultByBatch extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private KeyCacheObject key;

    /** */
    private Map<KeyCacheObject, Map<UUID, GridCacheVersion>> dataMap;

    /** */
    private Map<UUID, NodePartitionSize> sizeMap;

    /** */
    public PartitionExecutionTaskResultByBatch() {

    }

    /** */
    public PartitionExecutionTaskResultByBatch(KeyCacheObject key,
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> dataMap,
        Map<UUID, NodePartitionSize> sizeMap) {
        this.key = key;
        this.dataMap = dataMap;
        this.sizeMap = sizeMap;
    }

    /** */
    public KeyCacheObject key() {
        return key;
    }

    /** */
    public void key(KeyCacheObject key) {
        this.key = key;
    }

    /** */
    public Map<KeyCacheObject, Map<UUID, GridCacheVersion>> dataMap() {
        return dataMap;
    }

    /** */
    public void dataMap(
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> dataMap) {
        this.dataMap = dataMap;
    }

    /** */
    public Map<UUID, NodePartitionSize> sizeMap() {
        return sizeMap;
    }

    /** */
    public void sizeMap(
        Map<UUID, NodePartitionSize> sizeMap) {
        this.sizeMap = sizeMap;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(key);
        out.writeObject(dataMap);
        out.writeObject(sizeMap);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        key = (KeyCacheObject)in.readObject();
        dataMap = (Map<KeyCacheObject, Map<UUID, GridCacheVersion>>)in.readObject();
        sizeMap = (Map<UUID, NodePartitionSize>)in.readObject();
    }
}

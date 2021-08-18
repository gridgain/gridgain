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
    public KeyCacheObject getKey() {
        return key;
    }

    /** */
    public void setKey(KeyCacheObject key) {
        this.key = key;
    }

    /** */
    public Map<KeyCacheObject, Map<UUID, GridCacheVersion>> getDataMap() {
        return dataMap;
    }

    /** */
    public void setDataMap(
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> dataMap) {
        this.dataMap = dataMap;
    }

    /** */
    public Map<UUID, NodePartitionSize> getSizeMap() {
        return sizeMap;
    }

    /** */
    public void setSizeMap(
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

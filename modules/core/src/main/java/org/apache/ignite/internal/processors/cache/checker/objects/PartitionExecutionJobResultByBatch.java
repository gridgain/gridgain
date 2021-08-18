package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/** */
public class PartitionExecutionJobResultByBatch extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private List<VersionedKey> versionedKeys;

    /** */
    private NodePartitionSize nodePartitionSize;

    /** */
    public PartitionExecutionJobResultByBatch() {

    }

    /** */
    public PartitionExecutionJobResultByBatch(List<VersionedKey> versionedKeys, NodePartitionSize nodePartitionSize) {
        this.versionedKeys = versionedKeys;
        this.nodePartitionSize = nodePartitionSize;
    }

    /** */
    public List<VersionedKey> versionedKeys() {
        return versionedKeys;
    }

    /** */
    public NodePartitionSize nodePartitionSize() {
        return nodePartitionSize;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(versionedKeys);
        out.writeObject(nodePartitionSize);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        versionedKeys = (List<VersionedKey>)in.readObject();
        nodePartitionSize = (NodePartitionSize)in.readObject();
    }
}

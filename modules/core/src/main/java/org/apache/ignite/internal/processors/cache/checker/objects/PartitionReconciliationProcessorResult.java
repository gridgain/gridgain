package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

public class PartitionReconciliationProcessorResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    private ReconciliationAffectedEntries reconciliationAffectedEntries;

    private Map<Integer, Map<Integer, Map<UUID, NodePartitionSize>>> partSizesMap;

    public PartitionReconciliationProcessorResult() {

    }

    public PartitionReconciliationProcessorResult(ReconciliationAffectedEntries reconciliationAffectedEntries,
        Map<Integer, Map<Integer, Map<UUID, NodePartitionSize>>> partSizesMap) {
        this.reconciliationAffectedEntries = reconciliationAffectedEntries;
        this.partSizesMap = partSizesMap;
    }

    public ReconciliationAffectedEntries getReconciliationAffectedEntries() {
        return reconciliationAffectedEntries;
    }

    public Map<Integer, Map<Integer, Map<UUID, NodePartitionSize>>> getPartSizesMap() {
        return partSizesMap;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(reconciliationAffectedEntries);
        out.writeObject(partSizesMap);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        reconciliationAffectedEntries = (ReconciliationAffectedEntries)in.readObject();
        partSizesMap = (Map<Integer, Map<Integer, Map<UUID, NodePartitionSize>>>)in.readObject();
    }
}

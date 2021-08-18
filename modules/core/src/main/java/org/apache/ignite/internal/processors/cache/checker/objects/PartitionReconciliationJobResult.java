package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/** */
public class PartitionReconciliationJobResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String filePath;

    /** */
    private ReconciliationAffectedEntries reconciliationAffectedEntries;

    /** */
    private Map<Integer, Map<Integer, Map<UUID, NodePartitionSize>>> partSizesMap;

    /** */
    private String errorMsg;

    /** */
    public PartitionReconciliationJobResult() {

    }

    /** */
    public PartitionReconciliationJobResult(String filePath,
        ReconciliationAffectedEntries reconciliationAffectedEntries,
        Map<Integer, Map<Integer, Map<UUID, NodePartitionSize>>> partSizesMap,
        String errorMsg) {
        this.filePath = filePath;
        this.reconciliationAffectedEntries = reconciliationAffectedEntries;
        this.partSizesMap = partSizesMap;
        this.errorMsg = errorMsg;
    }

    /** */
    public String getFilePath() {
        return filePath;
    }

    /** */
    public ReconciliationAffectedEntries getReconciliationAffectedEntries() {
        return reconciliationAffectedEntries;
    }

    /** */
    public Map<Integer, Map<Integer, Map<UUID, NodePartitionSize>>> getPartSizesMap() {
        return partSizesMap;
    }

    /** */
    public String getErrorMsg() {
        return errorMsg;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(filePath);
        out.writeObject(reconciliationAffectedEntries);
        out.writeObject(partSizesMap);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        filePath = (String)in.readObject();
        reconciliationAffectedEntries = (ReconciliationAffectedEntries)in.readObject();
        partSizesMap = (Map<Integer, Map<Integer, Map<UUID, NodePartitionSize>>>)in.readObject();
    }
}

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

/** */
public class PartitionReconciliationJobResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String filePath;

    /** */
    private ReconciliationAffectedEntries reconciliationAffectedEntries;

    /** */
    private Map<String, Map<Integer, Map<UUID, NodePartitionSize>>> partSizesMap;

    /** */
    private String errorMsg;

    /** */
    public PartitionReconciliationJobResult() {

    }

    /** */
    public PartitionReconciliationJobResult(String filePath,
        ReconciliationAffectedEntries reconciliationAffectedEntries,
        Map<String, Map<Integer, Map<UUID, NodePartitionSize>>> partSizesMap,
        String errorMsg) {
        this.filePath = filePath;
        this.reconciliationAffectedEntries = reconciliationAffectedEntries;
        this.partSizesMap = partSizesMap;
        this.errorMsg = errorMsg;
    }

    /** */
    public String filePath() {
        return filePath;
    }

    /** */
    public ReconciliationAffectedEntries reconciliationAffectedEntries() {
        return reconciliationAffectedEntries;
    }

    /** */
    public Map<String, Map<Integer, Map<UUID, NodePartitionSize>>> partSizesMap() {
        return partSizesMap;
    }

    /** */
    public String errorMsg() {
        return errorMsg;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(filePath);
        out.writeObject(reconciliationAffectedEntries);
        out.writeObject(partSizesMap);
        out.writeObject(errorMsg);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        filePath = (String)in.readObject();
        reconciliationAffectedEntries = (ReconciliationAffectedEntries)in.readObject();
        partSizesMap = (Map<String, Map<Integer, Map<UUID, NodePartitionSize>>>)in.readObject();
        errorMsg = (String)in.readObject();
    }
}

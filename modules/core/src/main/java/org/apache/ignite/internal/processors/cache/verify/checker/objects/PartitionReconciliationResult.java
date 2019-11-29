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

package org.apache.ignite.internal.processors.cache.verify.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationSkippedEntityHolder;
import org.apache.ignite.internal.util.typedef.internal.U;

public class PartitionReconciliationResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    private Map<String, Map<Integer, List<Map<UUID, PartitionReconciliationDataRowMeta>>>> inconsistentKeys;

    private Set<PartitionReconciliationSkippedEntityHolder<String>> skippedCaches;

    private Map<String, Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>>
        skippedEntries;

    /**
     * Default constructor for externalization.
     */
    public PartitionReconciliationResult() {
    }

    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType") public PartitionReconciliationResult (
        Map<String, Map<Integer, List<Map<UUID, PartitionReconciliationDataRowMeta>>>> inconsistentKeys) {
        this.inconsistentKeys = inconsistentKeys;
    }

    public PartitionReconciliationResult(
        Map<String, Map<Integer, List<Map<UUID, PartitionReconciliationDataRowMeta>>>> inconsistentKeys,
        Set<PartitionReconciliationSkippedEntityHolder<String>> skippedCaches,
        Map<String, Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>>
            skippedEntries) {
        this.inconsistentKeys = inconsistentKeys;
        this.skippedCaches = skippedCaches;
        this.skippedEntries = skippedEntries;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, inconsistentKeys);

        U.writeCollection(out, skippedCaches);

        U.writeMap(out, skippedEntries);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        inconsistentKeys = U.readMap(in);

        skippedCaches = U.readSet(in);

        skippedEntries = U.readMap(in);
    }

    public void print(Consumer<String> printer) {
        if (!inconsistentKeys.isEmpty()) {
            printer.accept("\nINCONSISTENT KEYS:\n\n");

            for (Map.Entry<String, Map<Integer, List<Map<UUID, PartitionReconciliationDataRowMeta>>>>
                cacheBoundedInconsistentKeysEntry : inconsistentKeys.entrySet()) {

                String cacheName = cacheBoundedInconsistentKeysEntry.getKey();

                for (Map.Entry<Integer, List<Map<UUID, PartitionReconciliationDataRowMeta>>> partitionBoundedInconsistentKeysEntry
                    : cacheBoundedInconsistentKeysEntry.getValue().entrySet()) {
                    for (Map<UUID, PartitionReconciliationDataRowMeta> inconsistentKey: partitionBoundedInconsistentKeysEntry.getValue()) {
                        StringBuilder recordBuilder = new StringBuilder();

                        Integer part = partitionBoundedInconsistentKeysEntry.getKey();

                        recordBuilder.append("Inconsistent key found: [cache='").append(cacheName).append("'");

                        recordBuilder.append(", partition=").append(part);

                        for (Map.Entry<UUID, PartitionReconciliationDataRowMeta> nodesBoundedInconsistentKeysEntry
                            : inconsistentKey.entrySet()) {
                            UUID nodeId = nodesBoundedInconsistentKeysEntry.getKey();

                            PartitionReconciliationDataRowMeta dataRow = nodesBoundedInconsistentKeysEntry.getValue();

                            recordBuilder.append(", nodeId=").append(nodeId);

                            recordBuilder.append(", dataRow=").append(dataRow);
                        }

                        recordBuilder.append("]\n");

                        printer.accept(recordBuilder.toString());
                    }
                }
            }
        }

        if (!skippedCaches.isEmpty()) {
            printer.accept("\nSKIPPED CACHES:\n\n");

            for (PartitionReconciliationSkippedEntityHolder<String> skippedCache : skippedCaches) {
                printer.accept("Following cache was skipped during partition reconciliation check cache=["
                    + skippedCache.skippedEntity() + "], reason=[" + skippedCache.skippingReason() + "]\n");
            }
        }

        if (!skippedEntries.isEmpty()) {
            printer.accept("\nSKIPPED ENTRIES:\n\n");

            for (Map.Entry<String, Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>>
                cacheBoundedSkippedEntries : skippedEntries.entrySet()) {
                String cacheName = cacheBoundedSkippedEntries.getKey();

                for (Map.Entry<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>
                    partitionBoundedSkippedEntries
                    : cacheBoundedSkippedEntries.getValue().entrySet()) {
                    StringBuilder recordBuilder = new StringBuilder();

                    Integer part = partitionBoundedSkippedEntries.getKey();

                    recordBuilder.append("Following entry was skipped [cache='").append(cacheName).append("'");

                    recordBuilder.append(", partition=").append(part);

                    for (PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta> skippedEntry
                        : partitionBoundedSkippedEntries.getValue()) {

                        recordBuilder.append(", entry=").append(skippedEntry.skippedEntity());

                        recordBuilder.append(", reason=").append(skippedEntry.skippingReason());
                    }
                    recordBuilder.append("]\n");

                    printer.accept(recordBuilder.toString());
                }
            }
        }
    }
}

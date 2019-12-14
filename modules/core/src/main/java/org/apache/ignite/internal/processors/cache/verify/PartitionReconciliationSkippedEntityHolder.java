/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

public class PartitionReconciliationSkippedEntityHolder<T> extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    private T skippedEntity;

    private SkippingReason skippingReason;

    public PartitionReconciliationSkippedEntityHolder() {
    }

    public PartitionReconciliationSkippedEntityHolder(T skippedEntity,
        SkippingReason skippingReason) {
        this.skippedEntity = skippedEntity;
        this.skippingReason = skippingReason;
    }

    @Override protected void
    writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(skippedEntity);
        U.writeEnum(out, skippingReason);
    }

    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        skippedEntity = (T) in.readObject();

        skippingReason = SkippingReason.fromOrdinal(in.readByte());
    }

    /**
     * @return Skipped entity.
     */
    public T skippedEntity() {
        return skippedEntity;
    }

    /**
     * @param skippedEntity New skipped entity.
     */
    public void skippedEntity(T skippedEntity) {
        this.skippedEntity = skippedEntity;
    }

    /**
     * @return Skipping reason.
     */
    public SkippingReason skippingReason() {
        return skippingReason;
    }

    /**
     * @param skippingReason New skipping reason.
     */
    public void skippingReason(SkippingReason skippingReason) {
        this.skippingReason = skippingReason;
    }

    public enum SkippingReason {
        /** */
        ENTITY_WITH_TTL("Given entity has ttl enabled.");

        private String reason;

        SkippingReason(String reason) {
            this.reason = reason;
        }

        /**
         * @return Reason.
         */
        public String reason() {
            return reason;
        }


        /** Enumerated values. */
        private static final SkippingReason[] VALS = values();

        /**
         * Efficiently gets enumerated value from its ordinal.
         *
         * @param ord Ordinal value.
         * @return Enumerated value or {@code null} if ordinal out of range.
         */
        @Nullable public static SkippingReason fromOrdinal(int ord) {
            return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
        }
    }
}

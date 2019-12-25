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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/**
 *
 */
public class ReconciliationPartialResult extends IgniteDataTransferObject {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /**
     *
     */
    private PartitionReconciliationResult partitionReconciliationResult;

    /**
     *
     */
    private String error;

    /**
     *
     */
    public ReconciliationPartialResult(String error) {
        this.error = error;
    }

    /**
     *
     */
    public ReconciliationPartialResult(PartitionReconciliationResult partReconciliationRes, String error) {
        this.partitionReconciliationResult = partReconciliationRes;
        this.error = error;
    }

    /**
     *
     */
    public ReconciliationPartialResult(PartitionReconciliationResult partReconciliationRes) {
        this.partitionReconciliationResult = partReconciliationRes;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(partitionReconciliationResult);
        out.writeObject(error);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        partitionReconciliationResult = (PartitionReconciliationResult)in.readObject();
        error = (String)in.readObject();
    }

    /**
     *
     */
    public PartitionReconciliationResult getPartitionReconciliationResult() {
        return partitionReconciliationResult;
    }

    /**
     *
     */
    public String getError() {
        return error;
    }
}

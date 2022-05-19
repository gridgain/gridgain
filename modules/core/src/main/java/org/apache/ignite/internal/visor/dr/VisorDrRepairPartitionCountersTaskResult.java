/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.visor.dr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * DR repair partition counters task results.
 */
public class VisorDrRepairPartitionCountersTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Exceptions. */
    private Map<UUID, Exception> exceptions;

    /** Results from cluster. */
    private Map<UUID, Collection<VisorDrRepairPartitionCountersJobResult>> results;

    /**
     * @param results Results.
     * @param exceptions Exceptions.
     */
    public VisorDrRepairPartitionCountersTaskResult(Map<UUID, Collection<VisorDrRepairPartitionCountersJobResult>> results,
        Map<UUID, Exception> exceptions) {
        this.exceptions = exceptions;
        this.results = results;
    }

    /**
     * For externalization only.
     */
    public VisorDrRepairPartitionCountersTaskResult() {
    }

    /**
     * @return Exceptions.
     */
    public Map<UUID, Exception> exceptions() {
        return exceptions;
    }

    /**
     * @return Results from cluster.
     */
    public Map<UUID, Collection<VisorDrRepairPartitionCountersJobResult>> results() {
        return results;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, exceptions);
        U.writeMap(out, results);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        exceptions = U.readMap(in);
        results = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrRepairPartitionCountersTaskResult.class, this);
    }
}
